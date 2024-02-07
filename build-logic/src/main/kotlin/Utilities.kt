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

import java.io.File
import java.util.Properties
import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.file.FileTree
import org.gradle.api.logging.LogLevel
import org.gradle.kotlin.dsl.getByType

fun Project.libsRequiredVersion(name: String): String {
  val libVer =
    extensions.getByType<VersionCatalogsExtension>().named("libs").findVersion(name).get()
  val reqVer = libVer.requiredVersion
  check(reqVer.isNotEmpty()) {
    "libs-version for '$name' is empty, but must not be empty, version. strict: ${libVer.strictVersion}, required: ${libVer.requiredVersion}, preferred: ${libVer.preferredVersion}"
  }
  return reqVer
}

fun testLogLevel(): String = System.getProperty("test.log.level", "WARN")

fun testLogLevel(minVerbose: String): String {
  val requested = LogLevel.valueOf(testLogLevel().uppercase())
  val minimum = LogLevel.valueOf(minVerbose.uppercase())
  if (requested.ordinal > minimum.ordinal) {
    return minimum.name
  }
  return requested.name
}

/** Utility method to check whether a Quarkus build shall produce the uber-jar. */
fun Project.quarkusFatJar(): Boolean = hasProperty("uber-jar")

fun Project.quarkusPackageType(): String = if (quarkusFatJar()) "uber-jar" else "fast-jar"

/** Just load [Properties] from a [File]. */
fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

class ReplaceInFiles(val files: FileTree, val replacements: Map<String, String>) : Action<Task> {
  override fun execute(task: Task) {
    files.forEach { f ->
      val src = f.readText()
      var txt = src
      for (e in replacements.entries) {
        txt = txt.replace(e.key, e.value)
      }
      if (txt != src) {
        f.writeText(txt)
      }
    }
  }
}
