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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ErrorCombiner implements AutoCloseable {
  private final Consumer<String> messageConsumer;

  private final Map<String, Integer> errors = new HashMap<>();

  public ErrorCombiner(Consumer<String> messageConsumer) {
    this.messageConsumer = messageConsumer;
  }

  @Override
  public void close() {
    flush();
  }

  public void push(String error) {
    synchronized (this) {
      errors.compute(error, (e, old) -> old == null ? 0 : 1 + old);
    }
  }

  private void flushInternal() {
    errors.forEach(
        (err, more) ->
            messageConsumer.accept(
                more > 0 ? err + "\n  (last message repeated " + more + " times)" : err));
    errors.clear();
  }

  public void flush() {
    synchronized (this) {
      flushInternal();
    }
  }
}
