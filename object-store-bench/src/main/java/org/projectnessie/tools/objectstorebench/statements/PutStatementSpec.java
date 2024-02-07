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

import static java.lang.Long.parseLong;
import static org.projectnessie.tools.objectstorebench.statements.ScriptParser.asSizeUnitMult;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.tools.objectstorebench.context.NamingStrategy;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

@Value.Immutable
public interface PutStatementSpec extends ObjectStatementSpec {

  @Value.Default
  default long bytes() {
    return 1024 * 1024;
  }

  @Value.Default
  default NamingStrategy namingStrategy() {
    return NamingStrategy.CONSTANT_PREFIX_NAME_GENERATOR;
  }

  @Override
  default Statement buildStatement() {
    return new PutStatementImpl(this);
  }

  static Builder builder() {
    return ImmutablePutStatementSpec.builder();
  }

  interface Builder extends ObjectStatementSpec.Builder<Builder, PutStatementSpec> {
    @CanIgnoreReturnValue
    Builder bytes(long bytes);

    @CanIgnoreReturnValue
    Builder namingStrategy(NamingStrategy namingStrategy);

    @CanIgnoreReturnValue
    default Builder from(ObjectStoreBenchParser.PutStatementContext ctx) {

      from(ctx.objectParams());

      ObjectStoreBenchParser.SizeExprContext size = ctx.sizeExpr();
      if (size != null) {
        bytes(parseLong(size.sizeValue.getText()) * asSizeUnitMult(size.sizeUnit.getText()));
      }

      namingStrategyFromContext(ctx.namingStrategyExpr()).ifPresent(this::namingStrategy);

      return this;
    }
  }
}
