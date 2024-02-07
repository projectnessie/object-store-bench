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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.objectstorebench.BaseGetPutIT;
import org.projectnessie.tools.objectstorebench.GetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

@QuarkusTest
@TestProfile(ITGetPutImplsGcs.FakeGCSResourceLifecycleManager.GCSTestProfile.class)
public class ITGetPutImplsGcs extends BaseGetPutIT {

  @ConfigProperty(name = "gcs.base-uri", defaultValue = "x")
  String endpoint;

  @ConfigProperty(name = "gcs.oauth2-token", defaultValue = "x")
  String token;

  @ConfigProperty(name = "gcs.project-id", defaultValue = "x")
  String projectId;

  @ConfigProperty(name = "gcs.bucket", defaultValue = "x")
  String bucket;

  GetPutOpts opts;

  @BeforeEach
  void setup() {
    opts = new GetPutOpts();
    opts.gcp.oauth2Token = Optional.of(token);
    opts.gcp.projectId = Optional.of(projectId);
    opts.baseUri = Optional.of(endpoint);
    opts.bucket = bucket;
  }

  @Test
  public void gcsGetPut() throws Exception {
    try (GetPut getPut = new GcsGetPut(opts)) {
      getPutImpl(getPut);
    }
  }

  public static class FakeGCSResourceLifecycleManager
      implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(FakeGCSResourceLifecycleManager.class);
    public static final int PORT = 4443;

    private GenericContainer<?> gcsContainer;

    private Optional<String> containerNetworkId;

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
      containerNetworkId = context.containerNetworkId();
    }

    protected static String dockerImage(String dbName) {
      URL resource = ITGetPutImplsGcs.class.getResource("Dockerfile-" + dbName + "-version");
      try (InputStream in = resource.openConnection().getInputStream()) {
        String[] imageTag =
            Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
                .map(String::trim)
                .filter(l -> l.startsWith("FROM "))
                .map(l -> l.substring(5).trim().split(":"))
                .findFirst()
                .orElseThrow();
        String image = imageTag[0];
        String version = System.getProperty("it.nessie.container." + dbName + ".tag", imageTag[1]);
        return image + ':' + version;
      } catch (Exception e) {
        throw new RuntimeException("Failed to extract tag from " + resource, e);
      }
    }

    @Override
    public Map<String, String> start() {
      gcsContainer = new GenericContainer<>(dockerImage("fake-gcs-server"));
      gcsContainer.withNetworkAliases(randomString("fake-gcs"));
      gcsContainer.withLogConsumer(
          c -> LOGGER.info("[FAKE-GCS] {}", c.getUtf8StringWithoutLineEnding()));
      gcsContainer.withCommand(
          "-scheme", "http", "-public-host", "127.0.0.1", "-log-level", "warn");
      gcsContainer.setWaitStrategy(
          new HttpWaitStrategy()
              .forPath("/storage/v1/b")
              .forPort(PORT)
              .withStartupTimeout(Duration.ofMinutes(2)));

      // It would be really nice to test GCS using the Fake GCS server and dynamic ports, but that's
      // impossible, because of the GCS upload protocol itself. GCS uploads are initiated using one
      // HTTP request, which returns a `Location` header, which contains the URL for the upload that
      // the client must use. Problem is that a Fake server running inside a Docker container cannot
      // infer the dynamically mapped port for this test, because the initial HTTP request does not
      // have a `Host` header.
      //
      gcsContainer.addExposedPort(PORT);
      //
      // STATIC PORT BINDING!
      gcsContainer.setPortBindings(singletonList(PORT + ":" + PORT));

      gcsContainer.start();

      String baseUri = "http://127.0.0.1:" + gcsPort();
      String oauth2token = randomString("token");
      String bucket = randomString("bucket");
      String project = randomString("project");

      LOGGER.info("Fake-GCS base URI: {}", baseUri);

      try (Storage storage =
          StorageOptions.http()
              .setHost(baseUri)
              .setCredentials(OAuth2Credentials.create(new AccessToken(oauth2token, null)))
              .setProjectId(project)
              .build()
              .getService()) {
        storage.create(BucketInfo.newBuilder(bucket).build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return ImmutableMap.of(
          "gcs.base-uri", baseUri,
          "gcs.oauth2-token", oauth2token,
          "gcs.project-id", project,
          "gcs.bucket", bucket);
    }

    private int gcsPort() {
      return gcsContainer.getMappedPort(PORT);
    }

    @Override
    public void stop() {
      if (gcsContainer != null) {
        try {
          gcsContainer.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          gcsContainer = null;
        }
      }
    }

    public static final class GCSTestProfile implements QuarkusTestProfile {
      @Override
      public List<TestResourceEntry> testResources() {
        return singletonList(new TestResourceEntry(FakeGCSResourceLifecycleManager.class));
      }
    }

    private static String randomString(String prefix) {
      return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
    }
  }
}
