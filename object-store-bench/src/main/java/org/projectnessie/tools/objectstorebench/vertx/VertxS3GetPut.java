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
package org.projectnessie.tools.objectstorebench.vertx;

import static java.time.Clock.systemUTC;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.tools.objectstorebench.Utils.formatIso8601DateTimeZulu;

import com.google.common.primitives.Ints;
import io.smallrye.mutiny.vertx.ReadStreamSubscriber;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.streams.WriteStream;
import java.io.InputStream;
import java.net.URI;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.projectnessie.tools.objectstorebench.AbstractGetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.signing.ChunkSigningResult;
import org.projectnessie.tools.objectstorebench.signing.RequestSigner;
import org.projectnessie.tools.objectstorebench.signing.SignerConfig;
import org.projectnessie.tools.objectstorebench.signing.SigningRequest;
import org.projectnessie.tools.objectstorebench.signing.SigningResult;
import org.projectnessie.tools.objectstorebench.time.TimerInstance;

public class VertxS3GetPut extends AbstractGetPut {

  private final RequestSigner requestSigner;
  private final Vertx vertx;
  private final HttpClient httpClient;
  private final String bucket;

  public VertxS3GetPut(GetPutOpts opts, Vertx vertx) {
    super(opts);
    this.vertx = vertx;
    this.bucket = opts.bucket;
    this.httpClient = produceHttpClient(vertx, opts.vertx);
    this.requestSigner =
        opts.s3.signatureVersion.newSigner(
            SignerConfig.builder()
                .accessKey(opts.s3.accessKey)
                .secretKey(opts.s3.secretKey)
                .region(region)
                .service("s3")
                .build());
  }

  @Override
  public String name() {
    return "Vert.x client (async)";
  }

  @Override
  public CompletionStage<RequestStats> doDelete(String object) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletionStage<RequestStats> doGet(String object) {
    URI uri = baseUri.resolve(bucket + '/' + object);

    RequestStats.Builder requestStats = RequestStats.builder().started(clock.wallClockMillis());
    TimerInstance timer = clock.newTimer();

    Future<HttpClientResponse> sent =
        httpClient
            .request(HttpMethod.GET, uri.getPort(), uri.getHost(), uri.getPath())
            .map(
                req -> {
                  TemporalAccessor timestamp = systemUTC().instant().atZone(ZoneId.of("Z"));
                  req.putHeader("x-amz-date", formatIso8601DateTimeZulu(timestamp));
                  req.putHeader("User-Agent", "Nessie-Object-Store-Bench");
                  req.putHeader(
                      "Host",
                      uri.getPort() != 0 ? uri.getHost() + ':' + uri.getPort() : uri.getHost());
                  requestSigner
                      .signRequest(
                          SigningRequest.builder()
                              .httpMethodName(req.getMethod().name())
                              .bucket(bucket)
                              .headers(new HttpReqHeaders(req))
                              .forcePathStyle(forcePathStyle)
                              .uri(uri)
                              .timestamp(timestamp)
                              .build())
                      .headersToSet()
                      .forEach(req::putHeader);
                  return req;
                })
            .compose(HttpClientRequest::send);

    return reqWithStats(
        bucket, object, sent, requestStats, timer, () -> {}, requestStats::contentLength);
  }

  private static class BufferWriteStream implements WriteStream<Buffer> {
    private final RequestStats.Builder requestStats;
    private final TimerInstance timer;
    private final LongConsumer bytesRead;
    private final AtomicLong bytes = new AtomicLong();

    public BufferWriteStream(
        RequestStats.Builder requestStats, TimerInstance timer, LongConsumer bytesRead) {
      this.requestStats = requestStats;
      this.timer = timer;
      this.bytesRead = bytesRead;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(@Nullable Handler<Throwable> handler) {
      return this;
    }

    @Override
    public Future<Void> write(Buffer data) {
      onData(data);
      return Future.succeededFuture();
    }

    @Override
    public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
      onData(data);
      handler.handle(Future.succeededFuture());
    }

    private void onData(Buffer data) {
      int length = data.length();
      if (bytes.getAndAdd(length) == 0) {
        requestStats.firstByteMicros(timer.elapsedMicros());
      }
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
      bytesRead.accept(bytes.get());
      requestStats.durationMicros(timer.elapsedMicros());
      if (handler != null) {
        handler.handle(Future.succeededFuture());
      }
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
      return this;
    }
  }

  private final class PutContext {
    final TimerInstance timer;
    final InputStream data;
    final int chunkSize;
    final RequestStats.Builder requestStats;
    final Semaphore semaphore = new Semaphore(0);
    final AtomicReference<ChunkSigningResult> signing = new AtomicReference<>();
    final AtomicReference<Flow.Subscriber<? super Buffer>> subscriberRef = new AtomicReference<>();

    PutContext(
        TimerInstance timer, InputStream data, int chunkSize, RequestStats.Builder requestStats) {
      this.timer = timer;
      this.data = data;
      this.chunkSize = chunkSize;
      this.requestStats = requestStats;
    }

    final Flow.Subscription subscription =
        new Flow.Subscription() {
          @Override
          public void request(long n) {
            semaphore.release(Ints.checkedCast(n));
          }

          @Override
          public void cancel() {
            Flow.Subscriber<? super Buffer> sub = subscriberRef.get();
            sub.onComplete();
            cancelRead();
          }
        };

    final AtomicBoolean cancelled = new AtomicBoolean();

    void cancelRead() {
      cancelled.set(true);
    }

    final Callable<Void> pushDataCallable =
        new Callable<>() {
          private long bytes;

          private ChunkSigningResult submitChunk(
              Flow.Subscriber<? super Buffer> subscriber,
              byte[] chunk,
              ChunkSigningResult signingResult)
              throws InterruptedException {

            signingResult = requestSigner.signChunk(signingResult, chunk);

            int bufferSizeHint =
                // max int-as-hex length
                8
                    // sig length
                    + 64
                    // 2x CR-LF
                    + 4
                    // string ";chunk-signature="
                    + 17
                    + chunk.length;
            Buffer buffer =
                Buffer.buffer(bufferSizeHint)
                    .appendString(Integer.toHexString(chunk.length))
                    .appendString(";chunk-signature=")
                    .appendString(signingResult.signature())
                    .appendString("\r\n")
                    .appendBytes(chunk)
                    .appendString("\r\n");

            bytes += chunk.length;

            semaphore.acquire(1);
            subscriber.onNext(buffer);

            return signingResult;
          }

          @Override
          public Void call() {
            Flow.Subscriber<? super Buffer> subscriber = subscriberRef.get();
            boolean first = true;
            boolean hadFinalZeroLength = false;
            try {
              ChunkSigningResult signingResult = requireNonNull(signing.get());
              while (true) {
                byte[] chunk = data.readNBytes(chunkSize);

                if (cancelled.get()) {
                  throw new InterruptedException("Write cancelled");
                }

                signingResult = submitChunk(subscriber, chunk, signingResult);
                if (first) {
                  first = false;
                  requestStats.firstByteMicros(timer.elapsedMicros());
                }

                if (chunk.length == 0) {
                  hadFinalZeroLength = true;
                  break;
                }
                if (chunk.length < chunkSize) {
                  break;
                }
              }

              if (!hadFinalZeroLength) {
                submitChunk(subscriber, new byte[0], signingResult);
              }
            } catch (Exception e) {
              subscriber.onError(e);
            }

            requestStats.contentLength(bytes);
            subscriber.onComplete();

            return null;
          }
        };
  }

  @Override
  public CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize) {
    URI uri = baseUri.resolve(bucket + '/' + object);

    PutContext putContext = new PutContext(clock.newTimer(), data, 131072, RequestStats.builder());

    String contentType = "application/octet-stream";

    Promise<HttpClientResponse> clientResponsePromise = Promise.promise();

    Future<HttpClientResponse> sent =
        httpClient
            .request(HttpMethod.PUT, uri.getPort(), uri.getHost(), uri.getPath())
            .compose(
                req -> {
                  TemporalAccessor timestamp = systemUTC().instant().atZone(ZoneId.of("Z"));
                  req.putHeader("x-amz-date", formatIso8601DateTimeZulu(timestamp));
                  req.putHeader("Content-Encoding", "aws-chunked");
                  req.putHeader("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
                  req.putHeader("Content-Length", Long.toString(dataSize));
                  req.putHeader("Content-Type", contentType);
                  req.putHeader("Transfer-Encoding", "identity");

                  req.putHeader("Expect", "100-Continue");
                  req.putHeader("Connection", "keep-alive");

                  // TODO 100/continue
                  req.putHeader("amz-sdk-invocation-id", UUID.randomUUID().toString());
                  req.putHeader("amz-sdk-request", "attempt=1; max=4");
                  req.putHeader("accept", "*/*");

                  req.putHeader(
                      "Host",
                      uri.getPort() != 0 ? uri.getHost() + ':' + uri.getPort() : uri.getHost());

                  SigningResult signingResult =
                      requestSigner.signRequest(
                          SigningRequest.builder()
                              .httpMethodName(req.getMethod().name())
                              .bucket(bucket)
                              .headers(new HttpReqHeaders(req))
                              .forcePathStyle(forcePathStyle)
                              .uri(uri)
                              .timestamp(timestamp)
                              .chunkLength(putContext.chunkSize)
                              .build());
                  putContext.signing.set(signingResult.asChunkSigningResult());
                  signingResult.headersToSet().forEach(req::putHeader);

                  req.putHeader("User-Agent", "Nessie-Object-Store-Bench");

                  return req.send(
                      ReadStreamSubscriber.asReadStream(
                          subscriber -> {
                            if (!putContext.subscriberRef.compareAndSet(null, subscriber)) {
                              subscriber.onError(
                                  new IllegalStateException("Only one subscription allowed"));
                            }
                            subscriber.onSubscribe(putContext.subscription);

                            // start piping data
                            vertx.executeBlocking(putContext.pushDataCallable);
                          },
                          Function.identity()));

                  // TODO HTTP/100-continue support, see
                  //  https://vertx.io/docs/vertx-core/java/#_100_continue_handling
                  //                  req.continueHandler(
                  //                      x -> {
                  //                        System.err.println("CONTINUE :)");
                  //                        req.send(
                  //                                ReadStreamSubscriber.asReadStream(
                  //                                    subscriber -> {
                  //                                      if
                  // (!putContext.subscriberRef.compareAndSet(
                  //                                          null, subscriber)) {
                  //                                        subscriber.onError(
                  //                                            new IllegalStateException(
                  //                                                "Only one subscription
                  // allowed"));
                  //                                      }
                  //
                  // subscriber.onSubscribe(putContext.subscription);
                  //
                  //                                      // start piping data
                  //
                  // vertx.executeBlocking(putContext.pushDataCallable);
                  //                                    },
                  //                                    Function.identity()))
                  //                            .onSuccess(clientResponsePromise::complete)
                  //                            .onFailure(clientResponsePromise::fail);
                  //                      });
                  //
                  //                  System.err.println("sendHead");
                  //
                  //                  return req.sendHead()
                  //                      .compose(
                  //                          x -> {
                  //                            System.err.println("SEND HEAD");
                  //                            return clientResponsePromise.future();
                  //                          });
                });

    return reqWithStats(
        bucket,
        object,
        sent,
        putContext.requestStats,
        putContext.timer,
        putContext::cancelRead,
        cl -> {});
  }

  private CompletionStage<RequestStats> reqWithStats(
      String bucket,
      String object,
      Future<HttpClientResponse> sent,
      RequestStats.Builder requestStats,
      TimerInstance timer,
      Runnable cancel,
      LongConsumer bytesRead) {
    requestStats.started(clock.wallClockMillis());
    WriteStream<Buffer> writeStream = new BufferWriteStream(requestStats, timer, bytesRead);

    return sent.compose(
            r -> {
              requestStats.status(r.statusCode());
              if (r.statusCode() != 200) {
                cancel.run();
                return r.body()
                    .map(
                        b ->
                            requestStats
                                .durationMicros(timer.elapsedMicros())
                                .failure(
                                    new RuntimeException(
                                        "Failed request to "
                                            + baseUri
                                            + ", bucket "
                                            + bucket
                                            + ", object "
                                            + object
                                            + " : HTTP/"
                                            + r.statusCode()
                                            + " "
                                            + r.statusMessage()
                                            + ":\n"
                                            + b.toString())))
                    .map(x -> null);
              }
              return r.pipeTo(writeStream);
            })
        .map(x -> requestStats.build())
        .onFailure(t -> requestStats.failure(t).durationMicros(timer.elapsedMicros()))
        .toCompletionStage();
  }

  @Override
  public void close() throws Exception {
    try {
      httpClient.close().result();
    } finally {
      super.close();
    }
  }

  static HttpClient produceHttpClient(Vertx vertx, GetPutOpts.VertxOpts opts) {
    try {
      HttpClientOptions httpClientOptions = new HttpClientOptions();
      opts.connectTimeout.ifPresent(httpClientOptions::setConnectTimeout);
      opts.http2MaxPoolSize.ifPresent(httpClientOptions::setHttp2MaxPoolSize);
      opts.http2KeepAliveTimeout.ifPresent(httpClientOptions::setHttp2KeepAliveTimeout);
      opts.http2MultiplexingLimit.ifPresent(httpClientOptions::setHttp2MultiplexingLimit);
      opts.idleTimeout.ifPresent(httpClientOptions::setIdleTimeout);
      opts.keepAlive.ifPresent(httpClientOptions::setKeepAlive);
      opts.keepAliveTimeout.ifPresent(httpClientOptions::setKeepAliveTimeout);
      opts.maxChunkSize.ifPresent(httpClientOptions::setMaxChunkSize);
      opts.maxPoolSize.ifPresent(httpClientOptions::setMaxPoolSize);
      opts.pipelining.ifPresent(httpClientOptions::setPipelining);
      opts.protocolVersion.ifPresent(httpClientOptions::setProtocolVersion);
      opts.readIdleTimeout.ifPresent(httpClientOptions::setReadIdleTimeout);
      return vertx.createHttpClient(httpClientOptions);
    } catch (RuntimeException e) {
      try {
        vertx.close();
      } catch (Exception x) {
        e.addSuppressed(x);
      }
      throw e;
    }
  }
}
