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
import org.immutables.value.Value;
import org.projectnessie.tools.objectstorebench.syntax.ObjectStoreBenchParser;

@Value.Immutable
public interface GetStatementSpec extends ObjectStatementSpec {

  @Override
  default Statement buildStatement() {
    return new GetStatementImpl(this);
  }

  static Builder builder() {
    return ImmutableGetStatementSpec.builder();
  }

  interface Builder extends ObjectStatementSpec.Builder<Builder, GetStatementSpec> {

    @CanIgnoreReturnValue
    default Builder from(ObjectStoreBenchParser.GetStatementContext ctx) {

      from(ctx.objectParams());

      return this;
    }
  }
}
