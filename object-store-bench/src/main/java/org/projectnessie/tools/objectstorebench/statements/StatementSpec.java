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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import org.projectnessie.tools.objectstorebench.context.NamingStrategy;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

public interface StatementSpec {

  String description();

  Statement buildStatement();

  interface Builder<B extends Builder<B, C>, C extends StatementSpec> {
    @CanIgnoreReturnValue
    B description(String description);

    C build();

    default Optional<NamingStrategy> namingStrategyFromContext(
        ObjectStoreBenchParser.NamingStrategyExprContext namingStrategyExprContext) {
      return Optional.ofNullable(namingStrategyExprContext)
          .map(
              naming -> {
                if (naming.nsConstant != null) {
                  return naming.seed == null
                      ? NamingStrategy.CONSTANT_PREFIX_NAME_GENERATOR
                      : new NamingStrategy.SeededConstantPrefixNamingStrategy(
                          naming.seed.nameLiteralValue.getText());
                } else if (naming.nsRandmom != null) {
                  return naming.seed == null
                      ? NamingStrategy.RANDOM_PREFIX_NAME_GENERATOR
                      : new NamingStrategy.SeededRandomPrefixNamingStrategy(
                          naming.seed.nameLiteralValue.getText());
                } else {
                  throw new IllegalStateException("Unimplemented naming strategy");
                }
              });
    }
  }
}
