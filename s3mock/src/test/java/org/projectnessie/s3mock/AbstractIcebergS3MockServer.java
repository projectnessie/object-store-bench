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
package org.projectnessie.s3mock;

import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.s3mock.IcebergS3Mock.S3MockServer;

public class AbstractIcebergS3MockServer {
  private S3MockServer serverInstance;

  protected S3MockServer createServer(Consumer<ImmutableIcebergS3Mock.Builder> configure) {
    if (serverInstance == null) {
      ImmutableIcebergS3Mock.Builder serverBuilder = IcebergS3Mock.builder();

      configure.accept(serverBuilder);

      serverInstance = serverBuilder.build().start();

      onCreated(serverInstance);

      return serverInstance;
    } else {
      throw new IllegalArgumentException("Server already created");
    }
  }

  protected void onCreated(S3MockServer serverInstance) {}

  @AfterEach
  public void stopServer() throws Exception {
    serverInstance.close();
  }
}
