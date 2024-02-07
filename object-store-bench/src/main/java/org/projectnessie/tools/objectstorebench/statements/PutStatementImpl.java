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

import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.context.ExecutionContext;
import org.projectnessie.tools.objectstorebench.context.NamingStrategy;
import org.projectnessie.tools.objectstorebench.context.ObjectsContext;

final class PutStatementImpl extends ObjectStatementImpl<PutStatementSpec> {

  PutStatementImpl(PutStatementSpec putStatementSpec) {
    super(putStatementSpec);
  }

  @Override
  protected CompletionStage<RequestStats> startOperation(
      ExecutionContext executionContext, StatementContext statementContext) {
    InputStream in =
        new InputStream() {
          long remain = statementSpec.bytes();
          final Random random = new Random();

          @Override
          public int read() {
            if (remain <= 0L) {
              return -1;
            }
            return random.nextInt(255);
          }

          @Override
          public int read(byte[] b, int off, int len) {
            if (remain <= 0L) {
              return -1;
            }
            int l = (int) Math.min(len, remain);
            if (off == 0 && b.length == l) {
              random.nextBytes(b);
            } else {
              l = Math.min(l, 10 * 1024 * 1024); // limit heap pressure, just in case
              byte[] tmp = new byte[l];
              random.nextBytes(tmp);
              System.arraycopy(tmp, 0, b, off, l);
            }
            remain -= l;
            return l;
          }

          @Override
          public int available() {
            if (remain > Integer.MAX_VALUE) {
              return Integer.MAX_VALUE;
            }
            return (int) remain;
          }
        };
    return executionContext
        .getPut()
        .doPut(statementContext.nextObject(), in, statementSpec.bytes());
  }

  @Override
  protected StatementContext buildStatementContext(ExecutionContext executionContext) {
    NamingStrategy naming = statementSpec.namingStrategy();
    ObjectsContext objectsContext =
        executionContext.getOrCreateObjectsContext(statementSpec.objectContextName(), naming);

    executionContext.printInfo(
        String.format("Naming strategy '%s' uses seed \"%s\"", naming.name(), naming.seed()));

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
            String object;
            if (i < numObjects) {
              objectCount++;
              object = objectsContext.create();
            } else {
              object = objectsContext.getRandom();
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
        // Finish predicate: all objects processed
        // Objects count limited.
        return new StatementContext() {
          int objectCount;

          @Override
          public String nextObject() {
            objectCount++;
            return objectsContext.create();
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

        // Finish predicate: duration elapsed.
        // Objects count: unlimited!
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
            objectCount++;
            return objectsContext.create();
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
            "PUT statement must use the duration clause, the object-count clause or both.");
      }
    }
  }
}
