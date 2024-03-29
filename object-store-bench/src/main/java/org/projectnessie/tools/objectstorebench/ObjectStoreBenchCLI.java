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

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.util.OptionalInt;
import java.util.OptionalLong;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

@Command(
    name = "nessie-object-store-bench",
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class,
    subcommands = {Benchmark.class, HelpCommand.class})
public class ObjectStoreBenchCLI {
  public static void main(String[] arguments) {
    System.exit(runMain(arguments));
  }

  @VisibleForTesting
  public static int runMain(String[] arguments) {
    return runMain(null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, String[] arguments) {
    return runMain(out, null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, PrintWriter err, String[] arguments) {
    ObjectStoreBenchCLI command = new ObjectStoreBenchCLI();
    return runMain(command, out, err, arguments);
  }

  @VisibleForTesting
  public static int runMain(
      ObjectStoreBenchCLI command, PrintWriter out, PrintWriter err, String[] arguments) {
    CommandLine commandLine =
        new CommandLine(command)
            .registerConverter(OptionalInt.class, s -> OptionalInt.of(parseInt(s)))
            .registerConverter(OptionalLong.class, s -> OptionalLong.of(parseLong(s)))
            .setExecutionStrategy(command::execute)
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  return cmd.getExitCodeExceptionMapper() != null
                      ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                      : cmd.getCommandSpec().exitCodeOnExecutionException();
                });
    if (null != out) {
      commandLine = commandLine.setOut(out);
    }
    if (null != err) {
      commandLine = commandLine.setErr(err);
    }
    try {
      return commandLine.execute(arguments);
    } finally {
      commandLine.getOut().flush();
      commandLine.getErr().flush();
    }
  }

  protected int execute(ParseResult parseResult) {
    return new RunLast().execute(parseResult);
  }
}
