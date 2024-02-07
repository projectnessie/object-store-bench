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

plugins { `kotlin-dsl` }

dependencies {
  implementation(gradleKotlinDsl())
  implementation(baselibs.spotless)
  implementation(baselibs.jandex)
  implementation(baselibs.idea.ext)
  implementation(baselibs.shadow)
  implementation(baselibs.errorprone)

  testImplementation(platform(baselibs.junit.bom))
  testImplementation(baselibs.assertj.core)
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

java { toolchain { languageVersion = JavaLanguageVersion.of(11) } }

tasks.withType<Test>().configureEach { useJUnitPlatform() }
