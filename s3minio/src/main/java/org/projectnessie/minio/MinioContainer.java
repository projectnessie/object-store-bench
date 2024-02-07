/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.minio;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public final class MinioContainer extends GenericContainer<MinioContainer>
    implements MinioAccess, CloseableResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MinioContainer.class);

  private static final String DEFAULT_IMAGE;
  private static final String DEFAULT_TAG;
  private static final int S3_PORT = 9000;
  private static final int CONSOLE_PORT = 9001;

  static {
    URL resource = MinioContainer.class.getResource("Dockerfile-minio-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      DEFAULT_IMAGE =
          System.getProperty(
              "nessie.testing.minio.image",
              Optional.ofNullable(System.getenv("MINIO_DOCKER_IMAGE")).orElse(imageTag[0]));
      DEFAULT_TAG =
          System.getProperty(
              "nessie.testing.minio.tag",
              Optional.ofNullable(System.getenv("MINIO_DOCKER_TAG")).orElse(imageTag[1]));
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  private static final int DEFAULT_PORT = 9000;

  private static final String MINIO_ACCESS_KEY = "MINIO_ROOT_USER";
  private static final String MINIO_SECRET_KEY = "MINIO_ROOT_PASSWORD";
  private static final String MINIO_DOMAIN = "MINIO_DOMAIN";
  private static final String MINIO_CONSOLE_ADDRESS = "MINIO_CONSOLE_ADDRESS";

  private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
  private static final String HEALTH_ENDPOINT = "/minio/health/ready";
  private static final String MINIO_DOMAIN_NAME;
  private static final String MINIO_DOMAIN_NIP = "minio.127-0-0-1.nip.io";

  static boolean canRunOnMacOs() {
    return MINIO_DOMAIN_NAME.equals(MINIO_DOMAIN_NIP);
  }

  static {
    String name;
    try {
      InetAddress.getByName(MINIO_DOMAIN_NIP);
      name = MINIO_DOMAIN_NIP;
    } catch (UnknownHostException e) {
      LOGGER.warn(
          "Could not resolve '{}', falling back to 'localhost'. "
              + "This usually happens when your router or DNS provider is unable to resolve the nip.io addresses.",
          MINIO_DOMAIN_NIP);
      name = "localhost";
    }
    MINIO_DOMAIN_NAME = name;
  }

  private final String accessKey;
  private final String secretKey;
  private final String bucket;

  private String hostPort;
  private String consoleENdpoint;
  private String s3endpoint;
  private S3Client s3;
  private URI bucketBaseUri;

  @SuppressWarnings("unused")
  public MinioContainer() {
    this(null, null, null, null);
  }

  @SuppressWarnings("resource")
  public MinioContainer(String image, String accessKey, String secretKey, String bucket) {
    super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
    withNetworkAliases(randomString("minio"));
    withLogConsumer(c -> LOGGER.info("[MINIO] {}", c.getUtf8StringWithoutLineEnding()));
    addExposedPort(S3_PORT);
    addExposedPort(CONSOLE_PORT);
    this.accessKey = accessKey != null ? accessKey : randomString("access");
    this.secretKey = secretKey != null ? secretKey : randomString("secret");
    this.bucket = bucket != null ? bucket : randomString("bucket");
    withEnv(MINIO_ACCESS_KEY, this.accessKey);
    withEnv(MINIO_SECRET_KEY, this.secretKey);
    // S3 SDK encodes bucket names in host names - need to tell Minio which domain to use
    withEnv(MINIO_DOMAIN, MINIO_DOMAIN_NAME);
    withEnv(MINIO_CONSOLE_ADDRESS, ":" + CONSOLE_PORT);
    withCommand("server", DEFAULT_STORAGE_DIRECTORY);
    setWaitStrategy(
        new HttpWaitStrategy()
            .forPort(S3_PORT)
            .forPath(HEALTH_ENDPOINT)
            .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  @Override
  public String hostPort() {
    Preconditions.checkState(hostPort != null, "Container not yet started");
    return hostPort;
  }

  @Override
  public String accessKey() {
    return accessKey;
  }

  @Override
  public String secretKey() {
    return secretKey;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public String s3endpoint() {
    Preconditions.checkState(s3endpoint != null, "Container not yet started");
    return s3endpoint;
  }

  @Override
  public S3Client s3Client() {
    Preconditions.checkState(s3 != null, "Container not yet started");
    return s3;
  }

  @Override
  public URI s3BucketUri(String path) {
    Preconditions.checkState(bucketBaseUri != null, "Container not yet started");
    return bucketBaseUri.resolve(path);
  }

  @Override
  public void start() {
    super.start();

    this.hostPort = MINIO_DOMAIN_NAME + ":" + getMappedPort(S3_PORT);
    this.consoleENdpoint =
        String.format("http://%s:%d/", MINIO_DOMAIN_NAME, getMappedPort(CONSOLE_PORT));
    this.s3endpoint = String.format("http://%s/", hostPort);
    this.bucketBaseUri = URI.create(String.format("s3://%s/", bucket()));

    LOGGER.info(
        "Minio running, S3 endpoint {} - console at {} - access-key {} secret-key {}",
        s3endpoint,
        consoleENdpoint,
        accessKey,
        secretKey);

    this.s3 = createS3Client();
    this.s3.createBucket(CreateBucketRequest.builder().bucket(bucket()).build());
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public void stop() {
    try {
      if (s3 != null) {
        s3.close();
      }
    } finally {
      s3 = null;
      super.stop();
    }
  }

  private S3Client createS3Client() {
    return S3Client.builder()
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .applyMutation(builder -> builder.endpointOverride(URI.create(s3endpoint())))
        // .serviceConfiguration(s3Configuration(s3PathStyleAccess, s3UseArnRegionEnabled))
        // credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey(), secretKey())))
        .build();
  }
}
