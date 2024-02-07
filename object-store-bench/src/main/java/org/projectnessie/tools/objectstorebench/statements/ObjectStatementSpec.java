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

import static java.lang.Integer.parseInt;
import static org.projectnessie.tools.objectstorebench.statements.ScriptParser.asDuration;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

public interface ObjectStatementSpec extends StatementSpec {

  OptionalInt numObjects();

  OptionalInt maxConcurrent();

  Optional<RateLimit> rateLimit();

  Optional<Duration> warmup();

  Optional<Duration> runtime();

  @Value.Default
  default String objectContextName() {
    return "default";
  }

  interface Builder<B extends Builder<B, C>, C extends ObjectStatementSpec>
      extends StatementSpec.Builder<B, C> {
    @CanIgnoreReturnValue
    B numObjects(int numObjects);

    @CanIgnoreReturnValue
    B maxConcurrent(int maxConcurrent);

    @CanIgnoreReturnValue
    B rateLimit(RateLimit rateLimit);

    @CanIgnoreReturnValue
    B warmup(Duration warmup);

    @CanIgnoreReturnValue
    B runtime(Duration runtime);

    @CanIgnoreReturnValue
    B objectContextName(String objectContextName);

    @CanIgnoreReturnValue
    default B from(ObjectStoreBenchParser.ObjectParamsContext params) {
      ObjectStoreBenchParser.ContextExprContext context = params.contextExpr();
      if (context != null) {
        objectContextName(context.context.nameLiteralValue.getText());
      }

      ObjectStoreBenchParser.ConcurrentExprContext maxConcurrent = params.concurrentExpr();
      if (maxConcurrent != null) {
        maxConcurrent(parseInt(maxConcurrent.maxConcurrent.getText()));
      }

      ObjectStoreBenchParser.NumObjectsExprContext numObjects = params.numObjectsExpr();
      if (numObjects != null) {
        numObjects(parseInt(numObjects.numObjects.getText()));
      }

      ObjectStoreBenchParser.RateExprContext rate = params.rateExpr();
      if (rate != null) {
        rateLimit(RateLimit.builder().from(rate).build());
        asDuration(rate.warmup).ifPresent(this::warmup);
      }

      ObjectStoreBenchParser.RuntimeExprContext runtime = params.runtimeExpr();
      if (runtime != null) {
        asDuration(runtime.duration).ifPresent(this::runtime);
      }

      @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
      B me = (B) this;
      return me;
    }
  }
}
