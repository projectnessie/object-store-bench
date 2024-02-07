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

// Java project using Quarkus, Java 11 (soon: Java 17)

plugins {
  `java-library`
  id("nessie-common-base")
  id("nessie-common-src")
  id("nessie-java")
  id("nessie-testing")
}

tasks.withType<JavaCompile>().configureEach { options.release = 21 }
