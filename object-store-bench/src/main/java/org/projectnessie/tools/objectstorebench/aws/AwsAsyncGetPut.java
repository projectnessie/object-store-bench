/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.tools.objectstorebench.aws;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.Executors.newCachedThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.projectnessie.tools.objectstorebench.AbstractGetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.time.TimerInstance;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class AwsAsyncGetPut extends AbstractGetPut {
  private final S3AsyncClient client;
  private final ExecutorService executor;
  private final String name;
  private final String bucket;

  public AwsAsyncGetPut(
      GetPutOpts opts, String httpClientName, SdkAsyncHttpClient.Builder<?> httpClientBuilder) {
    super(opts);
    this.bucket = opts.bucket;
    this.name = "AWS client / " + httpClientName;
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    if (opts.s3.accessKey != null && opts.s3.secretKey != null) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(opts.s3.accessKey, opts.s3.secretKey)));
    } else {
      checkArgument(
          opts.s3.accessKey == null && opts.s3.secretKey == null,
          "Must specify S3 access-key and secret-key or none of both.");
    }
    if (baseUri != null) {
      builder.endpointOverride(baseUri);
    }
    this.client =
        builder
            .httpClientBuilder(httpClientBuilder)
            .forcePathStyle(forcePathStyle)
            .region(Region.of(region))
            .build();
    this.executor = newCachedThreadPool();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletionStage<RequestStats> doDelete(String object) {
    RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();
    return client
        .deleteObject(b -> b.bucket(bucket).key(object))
        .handle(
            (resp, error) -> {
              stats.durationMicros(timer.elapsedMicros());
              if (error != null) {
                handleException(error, stats);
              }
              if (resp != null) {
                stats.status(200);
              }
              return stats.build();
            });
  }

  @Override
  public CompletionStage<RequestStats> doGet(String object) {
    Consumer<GetObjectRequest.Builder> rb = b -> b.bucket(bucket).key(object);

    RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();
    return client
        .getObject(rb, AsyncResponseTransformer.toBlockingInputStream())
        .handle(
            (resp, error) -> {
              if (error != null) {
                handleException(error, stats);
              }
              if (resp != null) {
                stats
                    .status(200)
                    .contentLength(resp.response().contentLength())
                    .firstByteMicros(timer.elapsedMicros());

                try {
                  resp.transferTo(OutputStream.nullOutputStream());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
              stats.durationMicros(timer.elapsedMicros());
              return stats.build();
            });
  }

  @Override
  public CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize) {

    Consumer<PutObjectRequest.Builder> rb = b -> b.bucket(bucket).key(object);

    RequestStats.Builder stats =
        RequestStats.builder().started(clock.wallClockMillis()).contentLength(dataSize);

    TimerInstance timer = clock.newTimer();

    AsyncRequestBody requestBody =
        new AsyncRequestBody() {
          private long outstandingDemand;
          private volatile boolean done;
          private final Lock lock = new ReentrantLock();
          private final Condition demandChanged = lock.newCondition();

          @Override
          public Optional<Long> contentLength() {
            return Optional.of(dataSize);
          }

          @Override
          public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            Subscription subscription =
                new Subscription() {
                  @Override
                  public void request(long n) {
                    if (done) {
                      return;
                    }

                    if (n < 1) {
                      subscriber.onError(new IllegalArgumentException());
                      return;
                    }
                    lock.lock();
                    try {
                      if (Long.MAX_VALUE - outstandingDemand < n) {
                        outstandingDemand = Long.MAX_VALUE;
                      } else {
                        outstandingDemand += n;
                      }
                      demandChanged.signal();
                    } finally {
                      lock.unlock();
                    }
                  }

                  @Override
                  public void cancel() {
                    lock.lock();
                    try {
                      done = true;
                      demandChanged.signal();
                    } finally {
                      lock.unlock();
                    }
                  }
                };

            executor.submit(
                () -> {
                  try {
                    while (!done) {
                      lock.lock();
                      try {
                        if (outstandingDemand == 0L) {
                          demandChanged.await();
                        }
                        if (done) {
                          break;
                        }
                        if (outstandingDemand <= 0L) {
                          continue;
                        }
                        if (outstandingDemand != Long.MAX_VALUE) {
                          outstandingDemand--;
                        }
                      } finally {
                        lock.unlock();
                      }

                      byte[] buf = new byte[1024 * 1024];
                      int rd = data.read(buf, 0, buf.length);
                      if (rd < 0) {
                        done = true;
                        subscriber.onComplete();
                        break;
                      }
                      if (rd > 0) {
                        subscriber.onNext(ByteBuffer.wrap(buf, 0, rd));
                      }
                    }
                  } catch (Exception e) {
                    done = true;
                    subscriber.onError(e);
                  }
                });

            subscriber.onSubscribe(subscription);
          }
        };

    //    AsyncRequestBody requestBody =
    //        AsyncRequestBody.fromInputStream(
    //            new RequestStats.TimeToFirstByteInputStream(
    //                data, () -> stats.firstByteMicros(timer.elapsedMicros())),
    //            dataSize,
    //            executor);

    return client
        .putObject(rb, requestBody)
        .handle(
            (resp, error) -> {
              stats.durationMicros(timer.elapsedMicros());
              if (error != null) {
                handleException(error, stats);
              } else {
                stats.status(200);
              }
              return stats.build();
            });
  }

  private static void handleException(Throwable error, RequestStats.Builder stats) {
    if (error instanceof CompletionException) {
      error = error.getCause();
    }
    stats.failure(error);
    if (error instanceof AwsServiceException) {
      AwsServiceException ex = (AwsServiceException) error;
      AwsErrorDetails errorDetails = ex.awsErrorDetails();
      // omit AWS request ID + extended request ID from error message
      stats
          .status(ex.statusCode())
          .failureMessage(
              ex.getClass().getName()
                  + ": "
                  + errorDetails.errorMessage()
                  + " ("
                  + errorDetails.errorCode()
                  + ")");
    } else if (error instanceof SdkServiceException) {
      stats.status(((SdkServiceException) error).statusCode());
    } else {
      throw new RuntimeException(error);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      executor.shutdownNow();
      if (!executor.awaitTermination(2, TimeUnit.MINUTES)) {
        throw new RuntimeException(
            "Failed to shut down thread pool for synchronous AWS S3 requests.");
      }
    } finally {
      try {
        client.close();
      } finally {
        super.close();
      }
    }
  }
}
