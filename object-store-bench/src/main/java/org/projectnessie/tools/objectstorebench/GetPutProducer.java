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

import io.vertx.core.Vertx;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import java.util.Locale;
import org.projectnessie.tools.objectstorebench.aws.AwsNettyGetPut;
import org.projectnessie.tools.objectstorebench.aws.AwsUrlConnectionS3GetPut;
import org.projectnessie.tools.objectstorebench.vertx.VertxS3GetPut;
import picocli.CommandLine;

public class GetPutProducer {
  @Produces
  GetPut produceGetPut(CommandLine.ParseResult parseResult, Vertx vertx) {
    String implName = parseResult.matchedOption("--implementation").getValue().toString();
    switch (implName.toLowerCase(Locale.ROOT)) {
      case "awssync":
        return new AwsUrlConnectionS3GetPut(null);
      case "awsasync":
        return new AwsNettyGetPut(null);
      case "vertx":
        return new VertxS3GetPut(null, vertx);
      default:
        throw new CommandLine.ParameterException(
            parseResult.commandSpec().commandLine(), "Unknown implementation " + implName);
    }
  }

  void destroyGetPut(@Disposes GetPut getPut) throws Exception {
    getPut.close();
  }
}
