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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.tools.objectstorebench.context.NamingStrategy;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

@Value.Immutable
public interface ReuseStatementSpec extends StatementSpec {

  int numObjects();

  @Value.Default
  default NamingStrategy namingStrategy() {
    return NamingStrategy.CONSTANT_PREFIX_NAME_GENERATOR;
  }

  @Value.Default
  default String objectContextName() {
    return "default";
  }

  @Override
  default Statement buildStatement() {
    return new ReuseStatementImpl(this);
  }

  static Builder builder() {
    return ImmutableReuseStatementSpec.builder();
  }

  interface Builder extends StatementSpec.Builder<Builder, StatementSpec> {
    @CanIgnoreReturnValue
    Builder numObjects(int numObjects);

    @CanIgnoreReturnValue
    Builder namingStrategy(NamingStrategy namingStrategy);

    @CanIgnoreReturnValue
    Builder objectContextName(String objectContextName);

    @CanIgnoreReturnValue
    default Builder from(ObjectStoreBenchParser.ReuseStatementContext ctx) {

      ObjectStoreBenchParser.NumObjectsExprContext numObjects = ctx.numObjectsExpr();
      numObjects(parseInt(numObjects.numObjects.getText()));

      namingStrategyFromContext(ctx.namingStrategyExpr()).ifPresent(this::namingStrategy);

      return this;
    }
  }
}
