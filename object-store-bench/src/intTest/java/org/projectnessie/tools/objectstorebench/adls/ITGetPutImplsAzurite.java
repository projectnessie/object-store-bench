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

import com.azure.core.http.HttpClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceVersion;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.testing.azurite.AzuriteContainer;
import org.projectnessie.tools.objectstorebench.BaseGetPutIT;
import org.projectnessie.tools.objectstorebench.GetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;

@QuarkusTest
@TestProfile(ITGetPutImplsAzurite.AzuriteResourceLifecycleManager.AzuriteTestProfile.class)
@Disabled(
    """
It would be nice to have this integration test, but Azurite does NOT support ADLS (Gen2) yet. This results in request
errors like 'Incoming URL doesn't match any of swagger defined request patterns.' in the Azurite debug logs.
""")
public class ITGetPutImplsAzurite extends BaseGetPutIT {

  @ConfigProperty(name = "azurite.account.name", defaultValue = "x")
  String accountName;

  @ConfigProperty(name = "azurite.account.key", defaultValue = "x")
  String accountKey;

  @ConfigProperty(name = "azurite.file-system", defaultValue = "x")
  String filesystemName;

  @ConfigProperty(name = "azurite.endpoint", defaultValue = "x")
  String endpoint;

  GetPutOpts opts;

  @BeforeEach
  void setup() {
    opts = new GetPutOpts();
    opts.bucket = filesystemName;
    opts.adls.sharedKeyAccountName = Optional.of(accountName);
    opts.adls.sharedKeyAccountKey = Optional.of(accountKey);
    opts.baseUri = Optional.of(endpoint);
  }

  @Test
  public void azuriteGetPut() throws Exception {
    try (GetPut getPut = new AdlsGetPut(opts)) {
      getPutImpl(getPut);
    }
  }

  public static class AzuriteResourceLifecycleManager
      implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

    public static final String FILESYSTEM_NAME = "azureblobtest";
    private AzuriteContainer azuriteContainer;

    private Optional<String> containerNetworkId;

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
      containerNetworkId = context.containerNetworkId();
    }

    @Override
    public Map<String, String> start() {
      azuriteContainer = new AzuriteContainer(null);
      azuriteContainer.start();

      String endpoint = azuriteContainer.endpoint();
      DataLakeServiceClient client =
          new DataLakeServiceClientBuilder()
              .httpClient(HttpClient.createDefault())
              .credential(
                  new StorageSharedKeyCredential(
                      AzuriteContainer.ACCOUNT, AzuriteContainer.KEY_BASE64))
              .endpoint(endpoint)
              .serviceVersion(DataLakeServiceVersion.V2023_11_03)
              .buildClient();
      client.createFileSystem(FILESYSTEM_NAME);

      return ImmutableMap.of(
          "azurite.endpoint",
          endpoint,
          "azurite.file-system",
          FILESYSTEM_NAME,
          "azurite.account.name",
          AzuriteContainer.ACCOUNT,
          "azurite.account.key",
          AzuriteContainer.KEY_BASE64);
    }

    @Override
    public void stop() {
      if (azuriteContainer != null) {
        try {
          azuriteContainer.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          azuriteContainer = null;
        }
      }
    }

    public static final class AzuriteTestProfile implements QuarkusTestProfile {
      @Override
      public List<TestResourceEntry> testResources() {
        return Collections.singletonList(
            new TestResourceEntry(AzuriteResourceLifecycleManager.class));
      }
    }
  }
}
