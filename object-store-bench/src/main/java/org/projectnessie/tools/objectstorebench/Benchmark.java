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

import io.quarkus.picocli.runtime.annotations.TopCommand;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.projectnessie.tools.objectstorebench.adls.AdlsGetPut;
import org.projectnessie.tools.objectstorebench.aws.AwsApacheGetPut;
import org.projectnessie.tools.objectstorebench.aws.AwsNettyGetPut;
import org.projectnessie.tools.objectstorebench.aws.AwsUrlConnectionS3GetPut;
import org.projectnessie.tools.objectstorebench.context.ExecutionContext;
import org.projectnessie.tools.objectstorebench.gcs.GcsGetPut;
import org.projectnessie.tools.objectstorebench.statements.Script;
import org.projectnessie.tools.objectstorebench.statements.ScriptParseException;
import org.projectnessie.tools.objectstorebench.statements.ScriptParser;
import org.projectnessie.tools.objectstorebench.statements.StatementSpec;
import org.projectnessie.tools.objectstorebench.vertx.VertxS3GetPut;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@TopCommand
@Command(name = "bench", mixinStandardHelpOptions = true, description = "Run benchmark")
public class Benchmark implements Callable<Integer> {
  @Spec protected CommandSpec spec;

  @Mixin GetPutOpts getPutOptions;

  @Inject Vertx vertx;

  @Override
  public Integer call() {
    CommandLine command = spec.commandLine();

    PrintWriter out = command.getOut();
    PrintWriter err = command.getErr();

    Consumer<String> outConsumer =
        output -> {
          out.write(command.getColorScheme().ansi().string(output));
          out.flush();
        };
    Consumer<String> errConsumer =
        errStr -> {
          err.write(command.getColorScheme().errorText(errStr).toString());
          err.flush();
        };

    outConsumer.accept("""

@|bold,italic Initializing benchmark...|@
""");

    String bucket = getPutOptions.bucket;

    try (GetPut getPut = buildGetPut()) {
      outConsumer.accept(
          String.format(
              """
        implementation : @|bold %s |@
        region         : @|bold %s |@
        base uri       : @|bold %s |@
        bucket         : @|bold %s |@
        access key     : @|bold %s |@
        virtual threads: @|bold %s |@
      """,
              getPut.name(),
              getPut.region(),
              getPut.baseUri(),
              bucket,
              getPutOptions.s3.accessKey,
              getPutOptions.virtualThreads));

      Script script;
      if (getPutOptions.input.isPresent()) {
        try {
          script = ScriptParser.parseScriptFromFile(getPutOptions.input.get());
        } catch (FileNotFoundException | NoSuchFileException fnf) {
          err.printf("Script file %s not found.%n", getPutOptions.input.get());
          return 2;
        }
      } else if (getPutOptions.script.isPresent()) {
        script = ScriptParser.parseScriptFromString(getPutOptions.script.get());
      } else {
        err.printf("No input specified for the script to execute.%n");
        return 2;
      }

      ExecutionContext executionContext =
          new ExecutionContext(getPut, bucket, getPutOptions, outConsumer, errConsumer);
      for (StatementSpec statementSpec : script.statements()) {
        String stmtSrc =
            statementSpec.description().replaceAll("[\r\n]", " ").replaceAll("[ ]+", " ");
        outConsumer.accept(
            String.format(
                """

            Starting script statement: @|bold,underline %s|@ ...

            """,
                stmtSrc));
        statementSpec.buildStatement().execute(executionContext);
        outConsumer.accept(
            String.format(
                """
            Statement finished: @|bold,underline %s|@

            """,
                stmtSrc));
      }
    } catch (ScriptParseException e) {
      err.printf("Failed to parse script: %s", e.getMessage());
      return 2;
    } catch (Exception e) {
      err.println("Failure parsing script:");
      e.printStackTrace(err);
      return 2;
    }

    return 0;
  }

  private GetPut buildGetPut() {
    String implName = getPutOptions.implementation;
    return switch (implName.toLowerCase(Locale.ROOT)) {
      case "aws", "awsurl", "awsurlconnection" -> new AwsUrlConnectionS3GetPut(getPutOptions);
      case "awsapache" -> new AwsApacheGetPut(getPutOptions);
      case "awsnetty" -> new AwsNettyGetPut(getPutOptions);
      case "gcp", "gcs" -> new GcsGetPut(getPutOptions);
      case "azure", "adls" -> new AdlsGetPut(getPutOptions);
      case "vertx" -> new VertxS3GetPut(getPutOptions, vertx);
      default ->
          throw new CommandLine.ParameterException(
              spec.commandLine(), "Unknown implementation '" + implName + "'");
    };
  }
}
