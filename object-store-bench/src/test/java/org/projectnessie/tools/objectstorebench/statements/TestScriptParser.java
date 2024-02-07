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
package org.projectnessie.tools.objectstorebench.statements;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.tools.objectstorebench.context.NamingStrategy;

public class TestScriptParser {
  @ParameterizedTest
  @MethodSource
  public void parseScript(String script, Script expected) throws Exception {
    assertThat(ScriptParser.parseScriptFromString(script)).isEqualTo(expected);
  }

  static Stream<Arguments> parseScript() {
    return Stream.of(
        arguments("""
            """, Script.builder().build()),
        arguments(
            "PUT 250 OBJECTS USING NAMING STRATEGY RANDOM PREFIX",
            Script.builder()
                .addStatements(
                    PutStatementSpec.builder()
                        .description("PUT 250 OBJECTS USING NAMING STRATEGY RANDOM PREFIX")
                        .numObjects(250)
                        .namingStrategy(NamingStrategy.RANDOM_PREFIX_NAME_GENERATOR)
                        .build())
                .build()),
        arguments(
            "REUSE 10 OBJECTS USING NAMING STRATEGY CONSTANT PREFIX WITH SEED \"2e502943c7962ecbee99ace1ea07b27410de3b4ac9eba5d1740936380293a3fb\";",
            Script.builder()
                .addStatements(
                    ReuseStatementSpec.builder()
                        .description(
                            "REUSE 10 OBJECTS USING NAMING STRATEGY CONSTANT PREFIX WITH SEED \"2e502943c7962ecbee99ace1ea07b27410de3b4ac9eba5d1740936380293a3fb\"")
                        .numObjects(10)
                        .namingStrategy(
                            new NamingStrategy.SeededConstantPrefixNamingStrategy(
                                "2e502943c7962ecbee99ace1ea07b27410de3b4ac9eba5d1740936380293a3fb"))
                        .build())
                .build()),
        arguments(
            "PUT 250 OBJECTS;",
            Script.builder()
                .addStatements(
                    PutStatementSpec.builder()
                        .description("PUT 250 OBJECTS")
                        .numObjects(250)
                        .build())
                .build()),
        arguments(
            "GET AT RATE 50 PER SECOND RUNTIME 5 SECONDS;",
            Script.builder()
                .addStatements(
                    GetStatementSpec.builder()
                        .description("GET AT RATE 50 PER SECOND RUNTIME 5 SECONDS")
                        .rateLimit(
                            RateLimit.builder().rateValue(50).timeUnit(TimeUnit.SECONDS).build())
                        .runtime(Duration.of(5, ChronoUnit.SECONDS))
                        .build())
                .build()),
        arguments(
            """
           PUT
            250 OBJECTS;
           GET
             AT RATE 50 PER SECOND
             WARMUP 2 SECONDS
             RUNTIME 5 SECONDS
             IN CONTEXT "blah";
           GET
             MAX 10 CONCURRENT
             RUNTIME 5 SECONDS
             IN CONTEXT "blah";
           GET
             RUNTIME 3 SECONDS;
           """,
            Script.builder()
                .addStatements(
                    PutStatementSpec.builder()
                        .description(
                            """
                        PUT
                         250 OBJECTS""")
                        .numObjects(250)
                        .build())
                .addStatements(
                    GetStatementSpec.builder()
                        .description(
                            """
                        GET
                          AT RATE 50 PER SECOND
                          WARMUP 2 SECONDS
                          RUNTIME 5 SECONDS
                          IN CONTEXT "blah\"""")
                        .rateLimit(
                            RateLimit.builder().rateValue(50).timeUnit(TimeUnit.SECONDS).build())
                        .warmup(Duration.of(2, ChronoUnit.SECONDS))
                        .runtime(Duration.of(5, ChronoUnit.SECONDS))
                        .objectContextName("blah")
                        .build())
                .addStatements(
                    GetStatementSpec.builder()
                        .description(
                            """
                        GET
                          MAX 10 CONCURRENT
                          RUNTIME 5 SECONDS
                          IN CONTEXT "blah\"""")
                        .runtime(Duration.of(5, ChronoUnit.SECONDS))
                        .maxConcurrent(10)
                        .objectContextName("blah")
                        .build())
                .addStatements(
                    GetStatementSpec.builder()
                        .description(
                            """
                        GET
                          RUNTIME 3 SECONDS""")
                        .runtime(Duration.of(3, ChronoUnit.SECONDS))
                        .build())
                .build()));
  }
}
