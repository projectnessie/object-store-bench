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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.projectnessie.shaded.org.antlr.v4.runtime.BaseErrorListener;
import org.projectnessie.shaded.org.antlr.v4.runtime.CharStreams;
import org.projectnessie.shaded.org.antlr.v4.runtime.CommonTokenStream;
import org.projectnessie.shaded.org.antlr.v4.runtime.DefaultErrorStrategy;
import org.projectnessie.shaded.org.antlr.v4.runtime.NoViableAltException;
import org.projectnessie.shaded.org.antlr.v4.runtime.Parser;
import org.projectnessie.shaded.org.antlr.v4.runtime.RecognitionException;
import org.projectnessie.shaded.org.antlr.v4.runtime.Recognizer;
import org.projectnessie.shaded.org.antlr.v4.runtime.TokenStream;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchLexer;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

public final class ScriptParser {
  private ScriptParser() {}

  public static Optional<Duration> asDuration(ObjectStoreBenchParser.DurationExprContext ctx) {
    return ctx == null || ctx.isEmpty()
        ? Optional.empty()
        : Optional.of(
            Duration.of(
                Long.parseLong(ctx.durationValue.getText()),
                asChronoUnit(ctx.durationUnit.getText()).orElseThrow()));
  }

  public static Optional<TimeUnit> asRateUnit(String str) {
    if (str == null) {
      return Optional.empty();
    }
    return Optional.of(
        switch (str.toLowerCase(Locale.ROOT)) {
          case "hour", "hours" -> TimeUnit.HOURS;
          case "min", "minute", "minutes" -> TimeUnit.MINUTES;
          case "sec", "second", "seconds" -> TimeUnit.SECONDS;
          case "ms", "miillisecond", "miilliseconds" -> TimeUnit.MILLISECONDS;
          default -> throw new IllegalArgumentException("Illegal rate unit '" + str + "'");
        });
  }

  public static Optional<ChronoUnit> asChronoUnit(String str) {
    if (str == null) {
      return Optional.empty();
    }
    return Optional.of(
        switch (str.toLowerCase(Locale.ROOT)) {
          case "hour", "hours" -> ChronoUnit.HOURS;
          case "min", "minute", "minutes" -> ChronoUnit.MINUTES;
          case "sec", "second", "seconds" -> ChronoUnit.SECONDS;
          case "ms", "miillisecond", "miilliseconds" -> ChronoUnit.MILLIS;
          default -> throw new IllegalArgumentException("Illegal chrono unit '" + str + "'");
        });
  }

  public static long asSizeUnitMult(String text) {
    if (text == null) {
      return 1;
    }
    return switch (text.toLowerCase(Locale.ROOT)) {
      case "", "b", "byte", "bytes" -> 1;
      case "k", "kb", "kilobyte", "kilobytes" -> 1024L;
      case "m", "mb", "megabyte", "megabytes" -> 1024L * 1024;
      case "g", "gb", "gigabyte", "gigabytes" -> 1024L * 1024 * 1024;
      default -> throw new IllegalArgumentException(text);
    };
  }

  public static Script parseScriptFromFile(String input) throws IOException, ScriptParseException {
    if ("-".equalsIgnoreCase(input)) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      System.in.transferTo(buf);
      return parseScriptFromString(buf.toString(UTF_8));
    }

    return parseScriptFromString(Files.readString(Paths.get(input)));
  }

  public static Script parseScriptFromString(String source) throws ScriptParseException {
    TokenStream scriptStream =
        new CommonTokenStream(new ObjectStoreBenchLexer(CharStreams.fromString(source)));

    ObjectStoreBenchParser parser = new ObjectStoreBenchParser(scriptStream);

    // Add our own error listener
    parser.removeErrorListeners();
    List<String> errors = new ArrayList<>();
    parser.addErrorListener(
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              Recognizer<?, ?> recognizer,
              Object offendingSymbol,
              int line,
              int charPositionInLine,
              String msg,
              RecognitionException e) {
            errors.add(
                String.format("at line %d, column %d: %s%n", line, charPositionInLine + 1, msg));
          }
        });

    parser.setErrorHandler(
        new DefaultErrorStrategy() {
          @Override
          protected void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {
            TokenStream tokens = recognizer.getInputStream();
            String input;
            if (tokens != null) {
              if (e.getStartToken().getType() == -1) {
                input = "<EOF>";
              } else {
                input =
                    source.substring(
                        e.getStartToken().getStartIndex(),
                        e.getOffendingToken().getStopIndex() + 1);
              }
            } else {
              input = "<unknown input>";
            }

            String msg = "no viable alternative at input " + this.escapeWSAndQuote(input);
            recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
          }
        });

    Script.Builder scriptBuilder = Script.builder();

    ObjectStoreBenchParser.ScriptContext script = parser.script();
    if (script.exception != null) {
      String msg =
          errors.stream().collect(Collectors.joining("\n    ", "Script parse errors:\n    ", ""));
      throw new ScriptParseException(msg, errors);
    }
    for (ObjectStoreBenchParser.StatementContext statement : script.statement()) {
      StatementSpec.Builder<?, ?> statementBuilder = null;
      if (statement.putStatement() != null && !statement.putStatement().isEmpty()) {
        statementBuilder = PutStatementSpec.builder().from(statement.putStatement());
      }
      if (statement.getStatement() != null && !statement.getStatement().isEmpty()) {
        statementBuilder = GetStatementSpec.builder().from(statement.getStatement());
      }
      if (statement.deleteStatement() != null && !statement.deleteStatement().isEmpty()) {
        statementBuilder = DeleteStatementSpec.builder().from(statement.deleteStatement());
      }
      if (statement.reuseStatement() != null && !statement.reuseStatement().isEmpty()) {
        statementBuilder = ReuseStatementSpec.builder().from(statement.reuseStatement());
      }
      String statementSource =
          source.substring(
              statement.getStart().getStartIndex(), statement.getStop().getStopIndex() + 1);
      if (statementBuilder != null) {
        scriptBuilder.addStatements(statementBuilder.description(statementSource).build());
      } else {
        throw new IllegalArgumentException("Unknown statement: " + statementSource);
      }
    }

    return scriptBuilder.build();
  }
}
