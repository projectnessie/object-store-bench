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

plugins { id("nessie-conventions-root") }

extra["maven.name"] = "Object Store Bench"

description = "Tool to understand object-store request durations and rates and currency"

tasks.named<Wrapper>("wrapper").configure { distributionType = Wrapper.DistributionType.ALL }

spotless {
  kotlinGradle {
    // Must be repeated :( - there's no "addTarget" or so
    target("nessie-iceberg/*.gradle.kts", "*.gradle.kts", "build-logic/*.gradle.kts")
  }
}

tasks.named<Wrapper>("wrapper") {
  actions.addLast {
    val script = scriptFile.readText()
    val scriptLines = script.lines().toMutableList()

    val insertAtLine =
      scriptLines.indexOf("# Use the maximum available, or set MAX_FD != -1 to use that value.")
    scriptLines.add(insertAtLine, "")
    scriptLines.add(insertAtLine, $$". \"${APP_HOME}/gradle/gradlew-include.sh\"")

    scriptFile.writeText(scriptLines.joinToString("\n"))
  }
}
