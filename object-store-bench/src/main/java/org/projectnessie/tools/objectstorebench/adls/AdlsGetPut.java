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
package org.projectnessie.tools.objectstorebench.adls;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpClient;
import com.azure.core.util.BinaryData;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.projectnessie.tools.objectstorebench.AbstractGetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.time.TimerInstance;

public class AdlsGetPut extends AbstractGetPut {

  private final DataLakeFileSystemAsyncClient client;
  private final ParallelTransferOptions parallelTransferOptions;

  public AdlsGetPut(GetPutOpts opts) {
    super(opts);

    HttpClient httpClient =
        HttpClient.createDefault(
            new HttpClientOptions()
                .setReadTimeout(opts.http.readTimeout.orElse(null))
                .setResponseTimeout(opts.http.responseTimeout.orElse(null))
                .setConnectTimeout(opts.http.connectTimeout.orElse(null))
                .setWriteTimeout(opts.http.writeTimeout.orElse(null))
                .setMaximumConnectionPoolSize(opts.http.maxConnections.orElse(null)));

    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder().httpClient(httpClient);

    opts.baseUri.ifPresent(clientBuilder::endpoint);

    opts.adls.sasToken.ifPresent(clientBuilder::sasToken);
    opts.adls.sharedKeyAccountName.ifPresent(
        name ->
            clientBuilder.credential(
                new StorageSharedKeyCredential(
                    name,
                    opts.adls.sharedKeyAccountKey.orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                "Must specify shared key account key with name")))));
    if (opts.adls.sasToken.isEmpty() && opts.adls.sharedKeyAccountName.isEmpty()) {
      clientBuilder.credential(new DefaultAzureCredentialBuilder().build());
    }

    this.parallelTransferOptions =
        new ParallelTransferOptions()
            .setProgressListener(
                bytesTransferred ->
                    System.out.printf("Upload progress: %s bytes sent%n", bytesTransferred));

    opts.adls.writeBlockSize.ifPresent(parallelTransferOptions::setBlockSizeLong);
    opts.adls.writeMaxConcurrency.ifPresent(parallelTransferOptions::setMaxConcurrency);

    clientBuilder.fileSystemName(opts.bucket);

    this.client = clientBuilder.buildAsyncClient();
  }

  @Override
  public String name() {
    return "Azure ADLS";
  }

  @Override
  public CompletionStage<RequestStats> doGet(String object) {
    RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();
    AtomicLong size = new AtomicLong();
    CompletableFuture<RequestStats> cf = new CompletableFuture<>();
    getFileClient(object)
        .readWithResponse(null, null, null, false)
        .subscribe(
            response ->
                response
                    .getValue()
                    .subscribe(
                        new Consumer<>() {
                          private boolean first = true;

                          @Override
                          public void accept(ByteBuffer byteBuffer) {
                            if (first) {
                              stats.firstByteMicros(timer.elapsedMicros());
                              first = false;
                            }
                            size.addAndGet(byteBuffer.remaining());
                          }
                        }),
            e -> {
              if (e instanceof HttpResponseException) {
                stats.failure(e);
                stats.status(((HttpResponseException) e).getResponse().getStatusCode());
              } else {
                stats.failure(e);
              }
              cf.complete(stats.durationMicros(timer.elapsedMicros()).build());
            },
            () ->
                cf.complete(
                    stats.contentLength(size.get()).durationMicros(timer.elapsedMicros()).build()));
    return cf;
  }

  @Override
  public CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize) {
    RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();
    return getFileClient(object)
        .uploadWithResponse(
            new FileParallelUploadOptions(
                    BinaryData.fromStream(
                        new RequestStats.TimeToFirstByteInputStream(
                            data, () -> stats.firstByteMicros(timer.elapsedMicros())),
                        dataSize))
                .setParallelTransferOptions(parallelTransferOptions)
                .setHeaders(new PathHttpHeaders().setContentType("application/octet-stream")))
        .toFuture()
        .handle(
            (response, e) -> {
              if (response != null) {
                stats.status(response.getStatusCode()).contentLength(dataSize);
              }
              if (e instanceof HttpResponseException) {
                stats.failure(e);
                stats.status(((HttpResponseException) e).getResponse().getStatusCode());
              } else {
                stats.failure(e);
              }
              return stats.durationMicros(timer.elapsedMicros()).build();
            });
  }

  @Override
  public CompletionStage<RequestStats> doDelete(String object) {
    RequestStats.Builder stats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();
    return getFileClient(object)
        .deleteWithResponse(null)
        .toFuture()
        .handle(
            (response, e) -> {
              if (response != null) {
                stats.status(response.getStatusCode());
              }
              if (e instanceof HttpResponseException) {
                stats.failure(e);
                stats.status(((HttpResponseException) e).getResponse().getStatusCode());
              }
              return stats.durationMicros(timer.elapsedMicros()).build();
            });
  }

  private DataLakeFileAsyncClient getFileClient(String object) {
    return client.getFileAsyncClient(object);
  }
}
