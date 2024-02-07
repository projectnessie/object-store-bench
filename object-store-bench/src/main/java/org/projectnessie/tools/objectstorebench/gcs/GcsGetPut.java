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
package org.projectnessie.tools.objectstorebench.gcs;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newCachedThreadPool;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.projectnessie.tools.objectstorebench.AbstractGetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.time.TimerInstance;

public class GcsGetPut extends AbstractGetPut {
  private final Storage service;
  private final ExecutorService executor;
  private final String name;
  private final String bucket;

  public GcsGetPut(GetPutOpts opts, Storage storageService) {
    super(opts);
    this.bucket = opts.bucket;
    this.name = "Google Cloud Storage / Testing";
    this.service = storageService;
    this.executor =
        newCachedThreadPool(
            (opts.virtualThreads ? Thread.ofVirtual() : Thread.ofPlatform()).factory());
  }

  public GcsGetPut(GetPutOpts opts) {
    super(opts);
    this.bucket = opts.bucket;
    this.name = "Google Cloud Storage / " + (opts.gcp.grpc ? "gRPC" : "HTTP");

    StorageOptions.Builder builder;
    if (opts.gcp.grpc) {
      builder = StorageOptions.grpc();
      GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> executorFactory =
          new GrpcTransportOptions.ExecutorFactory<>() {
            @Override
            public ScheduledExecutorService get() {
              ScheduledThreadPoolExecutor service =
                  new ScheduledThreadPoolExecutor(
                      8,
                      (opts.virtualThreads ? Thread.ofVirtual() : Thread.ofPlatform()).factory());
              service.setKeepAliveTime(5L, TimeUnit.SECONDS);
              service.allowCoreThreadTimeOut(true);
              service.setRemoveOnCancelPolicy(true);
              return service;
            }

            @Override
            public void release(ScheduledExecutorService scheduledExecutorService) {
              scheduledExecutorService.shutdownNow();
              try {
                checkState(scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          };
      builder.setTransportOptions(
          GrpcTransportOptions.newBuilder().setExecutorFactory(executorFactory).build());
    } else {
      builder = StorageOptions.http();
      HttpTransportOptions.Builder httpOpts = HttpTransportOptions.newBuilder();
      opts.http
          .connectTimeout
          .map(Duration::toMillis)
          .map(Long::intValue)
          .ifPresent(httpOpts::setConnectTimeout);
      opts.http
          .readTimeout
          .map(Duration::toMillis)
          .map(Long::intValue)
          .ifPresent(httpOpts::setReadTimeout);
      builder.setTransportOptions(httpOpts.build());
    }

    opts.gcp.projectId.ifPresent(builder::setProjectId);
    opts.gcp.clientLibToken.ifPresent(builder::setClientLibToken);
    opts.baseUri.ifPresent(builder::setHost);

    if (!opts.gcp.auth.orElse(false)) {
      builder.setCredentials(NoCredentials.getInstance());
    } else {
      opts.gcp.oauth2Token.ifPresent(
          token -> {
            AccessToken accessToken =
                new AccessToken(token, opts.gcp.oauth2TokenExpiresAt.orElse(null));
            builder.setCredentials(OAuth2Credentials.create(accessToken));
          });
    }

    this.service = builder.build().getService();
    this.executor =
        newCachedThreadPool(
            (opts.virtualThreads ? Thread.ofVirtual() : Thread.ofPlatform()).factory());
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletionStage<RequestStats> doGet(String object) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
          TimerInstance timer = clock.newTimer();
          try {

            Blob blob = service.get(BlobId.of(bucket, object));
            stats.contentLength(blob.getSize());

            blob.downloadTo(
                new OutputStream() {
                  boolean first = true;

                  @Override
                  public void write(int b) {
                    if (first) {
                      stats.firstByteMicros(timer.elapsedMicros());
                      first = false;
                    }
                  }

                  @Override
                  public void write(byte[] b, int off, int len) {
                    if (len > 0 && first) {
                      stats.firstByteMicros(timer.elapsedMicros());
                      first = false;
                    }
                  }
                });

            stats.status(200);
          } catch (BaseHttpServiceException e) {
            stats.failure(e);
            stats.status(e.getCode());
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        });
  }

  @Override
  public CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats =
              RequestStats.builder().started(clock.wallClockMillis()).contentLength(dataSize);
          TimerInstance timer = clock.newTimer();
          try {
            InputStream input =
                new RequestStats.TimeToFirstByteInputStream(
                    data, () -> stats.firstByteMicros(timer.elapsedMicros()));
            BlobInfo info =
                BlobInfo.newBuilder(bucket, object)
                    .setContentType("application/octet-stream")
                    .build();
            service.createFrom(info, input);
            stats.status(200);
          } catch (BaseHttpServiceException e) {
            stats.failure(e);
            stats.status(e.getCode());
          } catch (IOException e) {
            throw new RuntimeException(e);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        });
  }

  @Override
  public CompletionStage<RequestStats> doDelete(String object) {
    return CompletableFuture.supplyAsync(
        () -> {
          RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
          TimerInstance timer = clock.newTimer();
          try {
            service.delete(BlobId.of(bucket, object));
            stats.status(200);
          } catch (BaseHttpServiceException e) {
            stats.failure(e);
            stats.status(e.getCode());
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            stats.durationMicros(timer.elapsedMicros());
          }
          return stats.build();
        });
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
      service.close();
    }
  }
}
