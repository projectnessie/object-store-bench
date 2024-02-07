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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.projectnessie.tools.objectstorebench.AbstractGetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.RequestStats.TimeToFirstByteInputStream;
import org.projectnessie.tools.objectstorebench.time.TimerInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class AwsGetPut extends AbstractGetPut {
  private final S3Client client;
  private final ExecutorService executor;
  private final String name;
  protected final String bucket;

  public AwsGetPut(
      GetPutOpts opts, String httpClientName, SdkHttpClient.Builder<?> httpClientBuilder) {
    super(opts);
    this.bucket = opts.bucket;
    this.name = "AWS client / " + httpClientName;
    S3ClientBuilder builder = S3Client.builder();
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
    this.executor =
        newCachedThreadPool(
            (opts.virtualThreads ? Thread.ofVirtual() : Thread.ofPlatform()).factory());
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletionStage<RequestStats> doDelete(String object) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
          TimerInstance timer = clock.newTimer();
          try {
            client.deleteObject(b -> b.bucket(bucket).key(object));
            stats.status(200);
          } catch (AwsServiceException ex) {
            // omit AWS request ID + extended request ID from error message
            AwsErrorDetails errorDetails = ex.awsErrorDetails();
            stats
                .status(ex.statusCode())
                .failureMessage(errorDetails.errorMessage() + " (" + errorDetails.errorCode() + ")")
                .failure(ex);
          } catch (SdkServiceException ex) {
            stats.status(ex.statusCode()).failure(ex);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        },
        executor);
  }

  @Override
  public CompletionStage<RequestStats> doGet(String object) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
          TimerInstance timer = clock.newTimer();
          try {
            try (ResponseInputStream<GetObjectResponse> result =
                client.getObject(b -> b.bucket(bucket).key(object))) {

              stats.firstByteMicros(timer.elapsedMicros());

              InputStream input =
                  new TimeToFirstByteInputStream(
                      result, () -> stats.firstByteMicros(timer.elapsedMicros()));
              byte[] buf = new byte[65536];
              while (input.read(buf) >= 0) {
                // no-op
              }

              GetObjectResponse resp = result.response();
              stats.status(200).contentLength(resp.contentLength());
            }
          } catch (AwsServiceException ex) {
            // omit AWS request ID + extended request ID from error message
            AwsErrorDetails errorDetails = ex.awsErrorDetails();
            stats
                .status(ex.statusCode())
                .failureMessage(errorDetails.errorMessage() + " (" + errorDetails.errorCode() + ")")
                .failure(ex);
          } catch (SdkServiceException ex) {
            stats.status(ex.statusCode()).failure(ex);
          } catch (IOException e) {
            throw new RuntimeException(e);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        },
        executor);
  }

  @Override
  public CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats =
              RequestStats.builder().started(clock.wallClockMillis()).contentLength(dataSize);
          TimerInstance timer = clock.newTimer();
          try {
            client.putObject(
                b -> b.bucket(bucket).key(object),
                RequestBody.fromInputStream(
                    new TimeToFirstByteInputStream(
                        data, () -> stats.firstByteMicros(timer.elapsedMicros())),
                    dataSize));
            stats.status(200);
          } catch (AwsServiceException ex) {
            // omit AWS request ID + extended request ID from error message
            AwsErrorDetails errorDetails = ex.awsErrorDetails();
            stats
                .status(ex.statusCode())
                .failureMessage(errorDetails.errorMessage() + " (" + errorDetails.errorCode() + ")")
                .failure(ex);
          } catch (SdkServiceException ex) {
            stats.status(ex.statusCode()).failure(ex);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        },
        executor);
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
