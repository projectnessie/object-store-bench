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

import java.util.concurrent.CompletionStage;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.context.ExecutionContext;
import org.projectnessie.tools.objectstorebench.context.ObjectsContext;

final class GetStatementImpl extends ObjectStatementImpl<GetStatementSpec> {
  public GetStatementImpl(GetStatementSpec getStatementSpec) {
    super(getStatementSpec);
  }

  @Override
  protected CompletionStage<RequestStats> startOperation(
      ExecutionContext executionContext, StatementContext statementContext) {
    return executionContext.getPut().doGet(statementContext.nextObject());
  }

  @Override
  protected StatementContext buildStatementContext(ExecutionContext executionContext) {
    ObjectsContext objectsContext =
        executionContext.getObjectsContext(statementSpec.objectContextName());

    if (statementSpec.numObjects().isPresent()) {
      int numObjects = statementSpec.numObjects().getAsInt();

      if (statementSpec.runtime().isPresent()) {
        long duration = statementSpec.runtime().get().toNanos();

        // Finish predicate: duration
        // Objects count limited.
        return new StatementContext() {
          int objectCount;
          long endNanos;

          @Override
          public StatementContext start(long nowNanos) {
            endNanos = nowNanos + duration;
            return this;
          }

          @Override
          public String nextObject() {
            int i = objectCount;
            String object = objectsContext.get(i);
            i++;
            if (i == numObjects) {
              objectCount = 0;
            }
            return object;
          }

          @Override
          public long endNanos() {
            return endNanos;
          }

          @Override
          public boolean finished(long nowNanos) {
            return nowNanos >= endNanos;
          }
        };

      } else {
        // Finish predicate: all objects processed.
        // Objects count limited.
        return new StatementContext() {
          int objectCount;

          @Override
          public String nextObject() {
            int i = objectCount++;
            return objectsContext.get(i);
          }

          @Override
          public long endNanos() {
            return Long.MAX_VALUE;
          }

          @Override
          public boolean finished(long nowNanos) {
            return objectCount >= numObjects;
          }
        };
      }
    } else {
      if (statementSpec.runtime().isPresent()) {
        long duration = statementSpec.runtime().get().toNanos();

        // Finish predicate: never!
        // Objects count: unlimited!
        return new StatementContext() {
          long endNanos;

          @Override
          public StatementContext start(long nowNanos) {
            endNanos = nowNanos + duration;
            return this;
          }

          @Override
          public String nextObject() {
            return objectsContext.getRandom();
          }

          @Override
          public long endNanos() {
            return endNanos;
          }

          @Override
          public boolean finished(long nowNanos) {
            return nowNanos >= endNanos;
          }
        };

      } else {
        throw new IllegalArgumentException(
            "GET statement must use the duration clause, the object-count clause or both.");
      }
    }
  }
}
