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
package org.projectnessie.tools.objectstorebench;

import static jakarta.validation.constraints.Pattern.Flag.CASE_INSENSITIVE;

import io.vertx.core.http.HttpVersion;
import jakarta.validation.constraints.Pattern;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import org.projectnessie.tools.objectstorebench.signing.SignatureVersion;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import software.amazon.awssdk.http.Protocol;

public class GetPutOpts {

  @Option(
      names = "--stats-interval",
      description = "Interval at which the request stats will be printed",
      defaultValue = "PT1S",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  public Duration statsInterval = Duration.of(1, ChronoUnit.SECONDS);

  //  @ArgGroup(exclusive = false, heading = "\nVert.x S3 (UNSUPPORTED)\n=========\n", order = 99)
  public VertxOpts vertx = new VertxOpts();

  @ArgGroup(exclusive = false, heading = "\nS3 Protocol\n===========\n", order = 20)
  public S3Opts s3 = new S3Opts();

  @ArgGroup(exclusive = false, heading = "\nGoogle Cloud\n============\n", order = 10)
  public GcpOpts gcp = new GcpOpts();

  @ArgGroup(exclusive = false, heading = "\nAzure ADSL\n============\n", order = 10)
  public AdlsOpts adls = new AdlsOpts();

  @ArgGroup(exclusive = false, heading = "\nHTTP options\n============\n", order = 0)
  public HttpOpts http = new HttpOpts();

  @Option(
      names = {"--implementation"},
      description =
          "The implementation to perform object store requests. Valid values are aws|awsUrlConnection|awsApache|awsNetty|gcs|gcp|adls|azure",
      defaultValue = "awsApache",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  @Pattern(
      regexp = "aws|awsUrlConnection|awsApache|awsNetty|gcs|gcp|adls|azure",
      flags = {CASE_INSENSITIVE})
  public String implementation = "awsApache";

  @Option(
      names = "--base-uri",
      description =
          "The object store (S3) endpoint URL. Defaults to the https endpoint corresponding to the configured cloud provider and region.")
  public Optional<String> baseUri = Optional.empty();

  @Option(
      names = {"-i", "--input"},
      description = "Benchmark script input.")
  public Optional<String> input = Optional.empty();

  @Option(
      names = {"--script"},
      description = "Benchmark script.")
  public Optional<String> script = Optional.empty();

  @Option(
      names = {"--bucket", "--adls-file-system-name"},
      defaultValue = "bucket",
      description = "The name of the S3 or GCS bucket, or the ADLS file system.",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  public String bucket = "bucket";

  @Option(
      names = {"--virtual-threads"},
      negatable = true)
  public boolean virtualThreads;

  public static class HttpOpts {
    // GCS/HTTP, AwsNetty, AwsApache, AwsUrlConnection
    @Option(names = {"--http-connect-timeout"})
    public Optional<Duration> connectTimeout = Optional.empty();

    // GCS/HTTP, AwsNetty, AwsApache, AwsUrlConnection
    @Option(names = {"--http-read-timeout"})
    public Optional<Duration> readTimeout = Optional.empty();

    // ADLS
    @Option(names = {"--http-response-timeout"})
    public Optional<Duration> responseTimeout = Optional.empty();

    // AwsNetty
    @Option(names = {"--http-write-timeout"})
    public Optional<Duration> writeTimeout = Optional.empty();

    // AwsNetty, AwsApache
    @Option(names = {"--http-connection-acquisition-timeout"})
    public Optional<Duration> connectionAcquisitionTimeout = Optional.empty();

    // AwsApache
    @Option(
        names = {"--http-expect-continue"},
        negatable = true)
    public Optional<Boolean> expectContinueEnabled = Optional.empty();

    // AwsNetty
    @Option(names = {"--http-max-concurrency"})
    public Optional<Integer> maxConcurrency = Optional.empty();

    // AwsNetty
    @Option(names = {"--http-max-http2-streams"})
    public Optional<Long> maxHttp2Streams = Optional.empty();

    // AwsApache
    @Option(names = {"--http-max-connections"})
    public Optional<Integer> maxConnections = Optional.empty();

    // AwsNetty
    @Option(names = {"--http-protocol"})
    public Optional<Protocol> protocol = Optional.empty();
  }

  public static class AdlsOpts {
    @Option(names = {"--adls-sas-token"})
    public Optional<String> sasToken = Optional.empty();

    @Option(names = {"--adls-block-size"})
    public Optional<Integer> blockSize = Optional.empty();

    @Option(names = {"--adls-write-block-size"})
    public Optional<Long> writeBlockSize = Optional.empty();

    @Option(names = {"--adls-write-max-concurrency"})
    public Optional<Integer> writeMaxConcurrency = Optional.empty();

    @Option(
        names = {"--adls-shared-key-account-name"},
        description = "Used for testing with the Azurite Container.")
    public Optional<String> sharedKeyAccountName = Optional.empty();

    @Option(
        names = {"--adls-shared-key-account-key"},
        description = "Used for testing with the Azurite Container.")
    public Optional<String> sharedKeyAccountKey = Optional.empty();
  }

  public static class GcpOpts {
    @Option(
        names = {"--gcp-grpc"},
        negatable = true)
    public boolean grpc;

    @Option(names = {"--gcp-project-id"})
    public Optional<String> projectId = Optional.empty();

    @Option(names = {"--gcp-client-lib-token"})
    public Optional<String> clientLibToken = Optional.empty();

    @Option(
        names = {"--gcp-auth"},
        negatable = true)
    public Optional<Boolean> auth = Optional.empty();

    @Option(names = {"--gcp-oauth-token"})
    public Optional<String> oauth2Token = Optional.empty();

    @Option(names = {"--gcp-oauth-token-expires-at"})
    public Optional<Date> oauth2TokenExpiresAt = Optional.empty();
  }

  public static class S3Opts {
    @Option(names = {"--s3-access-key"})
    public String accessKey;

    @Option(names = {"--s3-secret-key"})
    public String secretKey;

    @Option(
        names = {"--signature-version"},
        description =
            "The signature version to use to sign S3 requests, defaults to V4. Can be changed to V2, which is however not supported by most backends.",
        showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    public SignatureVersion signatureVersion = SignatureVersion.V4;

    @Option(
        names = {"--s3-region"},
        defaultValue = "eu-central-1")
    public Optional<String> region = Optional.of("eu-central-1");

    @Option(
        names = {"--s3-cloud"},
        defaultValue = "aws")
    @Pattern(
        regexp = "aws|amazon|google|gcp|gcs",
        flags = {CASE_INSENSITIVE})
    public Optional<String> cloud;
  }

  public static class VertxOpts {
    //  @Option(names = {"--vertx-max-connections"})
    //  public Optional<Integer> maxConnections = Optional.empty();

    // Controlled by Quarkus
    //  @Option(names = {"--vertx-worker-pool-size"})
    //  public Optional<Integer> workerPoolSize = Optional.empty();
    //
    //  @Option(names = {"--vertx-event-loop-pool-size"})
    //  public Optional<Integer> eventLoopPoolSize = Optional.empty();

    // Those values are available in newer Vert.x HTTP via PoolOptions
    //  @Option(names = {"--vertx-event-loop-size"})
    //  public Optional<Integer> eventLoopSize = Optional.empty();
    //
    //  @Option(names = {"--vertx-http-1-max-size"})
    //  public Optional<Integer> http1MaxSize = Optional.empty();
    //
    //  @Option(names = {"--vertx-http-2-max-size"})
    //  public Optional<Integer> http2MaxSize = Optional.empty();
    //
    //  @Option(names = {"--vertx-max-wait-queue-size"})
    //  public Optional<Integer> maxWaitQueueSize = Optional.empty();

    @Option(names = {"--vertx-connect-timeout"})
    public Optional<Integer> connectTimeout = Optional.empty();

    @Option(names = {"--vertx-http-2-max-pool-size"})
    public Optional<Integer> http2MaxPoolSize = Optional.empty();

    @Option(names = {"--vertx-http-2-keep-alive-timeout"})
    public Optional<Integer> http2KeepAliveTimeout = Optional.empty();

    @Option(names = {"--vertx-http-2-multiplexing-limit"})
    public Optional<Integer> http2MultiplexingLimit = Optional.empty();

    @Option(names = {"--vertx-idle-timeout"})
    public Optional<Integer> idleTimeout = Optional.empty();

    @Option(names = {"--vertx-keep-alive"})
    public Optional<Boolean> keepAlive = Optional.empty();

    @Option(names = {"--vertx-keep-alive-timeout"})
    public Optional<Integer> keepAliveTimeout = Optional.empty();

    @Option(names = {"--vertx-max-pool-size"})
    public Optional<Integer> maxPoolSize = Optional.empty();

    @Option(names = {"--vertx-max-chunk-size"})
    public Optional<Integer> maxChunkSize = Optional.empty();

    @Option(
        names = {"--vertx-pipelining"},
        negatable = true)
    public Optional<Boolean> pipelining = Optional.empty();

    @Option(names = {"--vertx-protocol-version"})
    public Optional<HttpVersion> protocolVersion = Optional.empty();

    @Option(names = {"--vertx-read-idle-timeout"})
    public Optional<Integer> readIdleTimeout = Optional.empty();
  }
}
