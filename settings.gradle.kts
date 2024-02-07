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

import java.util.Properties

includeBuild("build-logic") { name = "build-logic" }

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_21)) {
  throw GradleException("Build requires Java 21")
}

val baseVersion = file("version.txt").readText().trim()

pluginManagement {
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    mavenCentral()
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }
}

val groupIdMain = "org.projectnessie.nessie"

gradle.beforeProject {
  version = baseVersion
  group = "org.projectnessie.object-store-bench"
}

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun loadProjects(file: String) =
  loadProperties(file(file)).forEach { name, directory ->
    include(name as String)
    val p = project(":$name")
    p.name = name
    p.projectDir = file(directory as String)
  }

loadProjects("gradle/projects.main.properties")

val ideSyncActive =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    gradle.startParameter.taskNames.any { it.startsWith("eclipse") }

rootProject.name = "object-store-bench"
