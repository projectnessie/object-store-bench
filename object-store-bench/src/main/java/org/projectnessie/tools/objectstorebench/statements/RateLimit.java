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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.projectnessie.tools.objectstorebench.statements.ScriptParser.asRateUnit;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

@Value.Immutable
public interface RateLimit {
  OptionalDouble rateValue();

  @Value.Default
  default TimeUnit timeUnit() {
    return SECONDS;
  }

  static Builder builder() {
    return ImmutableRateLimit.builder();
  }

  default int perSecond() {
    if (rateValue().isEmpty()) {
      return Integer.MAX_VALUE;
    }

    // Max defined unit is HOUR in ObjectStoreScript.g4

    double value = rateValue().getAsDouble();

    return (int)
        switch (timeUnit()) {
          case HOURS -> value / TimeUnit.HOURS.toSeconds(1);
          case MINUTES -> value / TimeUnit.MINUTES.toSeconds(1);
          case SECONDS -> value;
          case MILLISECONDS -> value * SECONDS.toMillis(1);
          default -> throw new IllegalArgumentException("Unsupported rate unit " + timeUnit());
        };
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder rateValue(double rateValue);

    @CanIgnoreReturnValue
    Builder timeUnit(TimeUnit timeUnit);

    @CanIgnoreReturnValue
    default Builder from(ObjectStoreBenchParser.RateExprContext ctx) {
      rateValue(Double.parseDouble(ctx.rate.getText()));
      asRateUnit(ctx.rateUnit.getText()).ifPresent(this::timeUnit);

      return this;
    }

    RateLimit build();
  }
}
