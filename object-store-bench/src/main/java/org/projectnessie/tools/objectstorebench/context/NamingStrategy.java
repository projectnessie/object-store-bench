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
package org.projectnessie.tools.objectstorebench.context;

import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.ThreadLocalRandom;

public interface NamingStrategy {
  long PER_JVM_SEED = ThreadLocalRandom.current().nextLong();

  /**
   * Produces a name with a random prefix to avoid having objects in the same partition in an object
   * store.
   */
  NamingStrategy RANDOM_PREFIX_NAME_GENERATOR =
      new SeededRandomPrefixNamingStrategy(NamingStrategy.PER_JVM_SEED);

  /** Produces names with a long constant prefix. The prefix varies for each JVM. */
  NamingStrategy CONSTANT_PREFIX_NAME_GENERATOR =
      new SeededConstantPrefixNamingStrategy(NamingStrategy.PER_JVM_SEED);

  String generate(String contextName, int i);

  String seed();

  String name();

  @SuppressWarnings("UnstableApiUsage")
  final class SeededConstantPrefixNamingStrategy implements NamingStrategy {
    private static final String CONST_PREFIX =
        "/object-store-bench-" + sha256().hashString("", UTF_8) + "/";

    private final String seed;

    public SeededConstantPrefixNamingStrategy(long seed) {
      this(sha256().hashLong(seed).toString());
    }

    public SeededConstantPrefixNamingStrategy(String seed) {
      this.seed = seed;
    }

    @Override
    public String name() {
      return "CONSTANT PREFIX";
    }

    @Override
    public String seed() {
      return seed;
    }

    @Override
    public String generate(String contextName, int i) {
      return CONST_PREFIX + contextName + '/' + sha256().hashInt(i).toString() + "/object-" + seed;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SeededConstantPrefixNamingStrategy that = (SeededConstantPrefixNamingStrategy) o;
      return seed.equals(that.seed);
    }

    @Override
    public int hashCode() {
      return seed.hashCode();
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  final class SeededRandomPrefixNamingStrategy implements NamingStrategy {
    private final String seed;

    public SeededRandomPrefixNamingStrategy(long seed) {
      this(sha256().hashLong(seed).toString());
    }

    public SeededRandomPrefixNamingStrategy(String seed) {
      this.seed = seed;
    }

    @Override
    public String name() {
      return "RANDOM PREFIX";
    }

    @Override
    public String seed() {
      return seed;
    }

    @Override
    public String generate(String contextName, int i) {
      return "/" + seed + "/" + contextName + '/' + sha256().hashInt(i).toString() + "/object";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SeededRandomPrefixNamingStrategy that = (SeededRandomPrefixNamingStrategy) o;
      return seed.equals(that.seed);
    }

    @Override
    public int hashCode() {
      return seed.hashCode();
    }
  }
}
