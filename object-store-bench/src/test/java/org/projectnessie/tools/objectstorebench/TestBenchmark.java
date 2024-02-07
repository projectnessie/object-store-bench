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

import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
public class TestBenchmark extends BaseS3MockServer {
  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @AfterEach
  void softAssert() {
    soft.assertAll();
  }

  @Test
  @Launch(
      value = {"--implementation", "blah"},
      exitCode = 2)
  public void invalidImpl(LaunchResult result) {
    soft.assertThat(result.getErrorOutput()).contains("Unknown implementation 'blah'");
    soft.assertThat(result.exitCode()).isEqualTo(2);
  }

  @Test
  public void invalidScriptFile(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "awsUrlConnection",
            "--input",
            "/foo/bar/baz",
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : AWS client / URLConnection");
    soft.assertThat(result.exitCode()).isEqualTo(2);
  }

  @Test
  public void putAndGet(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "awsUrlConnection",
            "--script",
            """
              PUT 20 OBJECTS;
              GET AT RATE 10 PER SECOND RUNTIME 3 SECONDS;
              GET MAX 2 CONCURRENT RUNTIME 1 SECOND;
              DELETE;
            """,
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : AWS client / URLConnection");
    soft.assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  @Disabled
  public void vertx(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "vertx",
            "--script",
            "PUT 1 OBJECTS; DELETE;",
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : Vert.x client (async)");
    soft.assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void awsUrlConnection(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "awsUrlConnection",
            "--script",
            "PUT 1 OBJECTS; DELETE;",
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : AWS client / URLConnection");
    soft.assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void awsApache(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "awsApache",
            "--script",
            "PUT 1 OBJECTS; DELETE;",
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : AWS client / Apache");
    soft.assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void awsNetty(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "--implementation",
            "awsNetty",
            "--script",
            "PUT 1 OBJECTS; DELETE;",
            "--base-uri",
            opts.baseUri.get(),
            "--bucket",
            opts.bucket,
            "--s3-access-key",
            "access",
            "--s3-secret-key",
            "secret");

    soft.assertThat(result.getOutput()).contains("implementation : AWS client / Netty");
    soft.assertThat(result.exitCode()).isEqualTo(0);
  }
}
