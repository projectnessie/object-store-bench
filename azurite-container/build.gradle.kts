/*
 * Copyright (C) 2023 Dremio
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

plugins {
  `java-library`
  id("nessie-common-base")
  id("nessie-common-src")
  id("nessie-java")
  id("nessie-testing")
}

extra["maven.name"] = "Nessie - Azurite testcontainer"

dependencies {
  implementation(libs.slf4j.api)
  api(platform(libs.testcontainers.bom))
  api("org.testcontainers:testcontainers")

  api(platform(libs.azuresdk.bom))
  api("com.azure:azure-storage-file-datalake")
  api("com.azure:azure-identity")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)
  compileOnly(libs.errorprone.annotations)

  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
}

tasks.withType(Test::class.java).configureEach {
  environment("AZURE_USERNAME", "account")
  environment("AZURE_PASSWORD", "key")
}
