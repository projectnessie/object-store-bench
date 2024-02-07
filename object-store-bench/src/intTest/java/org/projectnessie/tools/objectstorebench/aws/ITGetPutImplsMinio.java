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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.tools.objectstorebench.BaseGetPutIT;
import org.projectnessie.tools.objectstorebench.GetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.vertx.VertxS3GetPut;
import software.amazon.awssdk.regions.Region;

@QuarkusTest
@TestProfile(ITGetPutImplsMinio.MinioResourceLifecycleManager.MinioTestProfile.class)
public class ITGetPutImplsMinio extends BaseGetPutIT {

  @Inject Vertx vertx;

  @ConfigProperty(name = "minio.accessKey", defaultValue = "x")
  String accessKey;

  @ConfigProperty(name = "minio.secretKey", defaultValue = "x")
  String secretKey;

  @ConfigProperty(name = "minio.bucket", defaultValue = "x")
  String bucket;

  @ConfigProperty(name = "minio.endpoint", defaultValue = "x")
  String endpoint;

  GetPutOpts opts;

  @BeforeEach
  void setup() {
    opts = new GetPutOpts();
    opts.bucket = bucket;
    opts.s3.region = Optional.of(Region.EU_CENTRAL_1.id());
    opts.s3.accessKey = accessKey;
    opts.s3.secretKey = secretKey;
    opts.baseUri = Optional.of(endpoint);
  }

  @Test
  @Disabled
  public void vertxGetPut() throws Exception {
    try (GetPut getPut = new VertxS3GetPut(opts, vertx)) {
      getPutImpl(getPut);
    }
  }

  @Test
  public void awsNettyGetPut() throws Exception {
    try (GetPut getPut = new AwsNettyGetPut(opts)) {
      getPutImpl(getPut);
    }
  }

  @Test
  public void awsUrlConnectionGetPut() throws Exception {
    try (GetPut getPut = new AwsUrlConnectionS3GetPut(opts)) {
      getPutImpl(getPut);
    }
  }

  @Test
  public void awsApacheGetPut() throws Exception {
    try (GetPut getPut = new AwsApacheGetPut(opts)) {
      getPutImpl(getPut);
    }
  }

  public static class MinioResourceLifecycleManager
      implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

    private MinioContainer minioContainer;

    private Optional<String> containerNetworkId;

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
      containerNetworkId = context.containerNetworkId();
    }

    @Override
    public Map<String, String> start() {

      minioContainer = new MinioContainer(null, null, null, null);

      minioContainer.start();

      return ImmutableMap.of(
          "minio.accessKey", minioContainer.accessKey(),
          "minio.secretKey", minioContainer.secretKey(),
          "minio.bucket", minioContainer.bucket(),
          "minio.endpoint", minioContainer.s3endpoint());
    }

    @Override
    public void stop() {
      if (minioContainer != null) {
        try {
          minioContainer.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          minioContainer = null;
        }
      }
    }

    public static final class MinioTestProfile implements QuarkusTestProfile {
      @Override
      public List<TestResourceEntry> testResources() {
        return Collections.singletonList(
            new TestResourceEntry(MinioResourceLifecycleManager.class));
      }
    }
  }
}
