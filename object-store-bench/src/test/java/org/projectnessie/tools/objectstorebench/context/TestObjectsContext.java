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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestObjectsContext {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void reuseNames(NamingStrategy strategy) {
    Set<String> fooNames = new HashSet<>();
    Set<String> barNames = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      fooNames.add(strategy.generate("foo", i));
      barNames.add(strategy.generate("bar", i));
    }

    soft.assertThat(fooNames).hasSize(100);
    soft.assertThat(barNames).hasSize(100);

    NamingStrategy similar =
        switch (strategy.name()) {
          case "CONSTANT PREFIX" ->
              new NamingStrategy.SeededConstantPrefixNamingStrategy(strategy.seed());
          case "RANDOM PREFIX" ->
              new NamingStrategy.SeededRandomPrefixNamingStrategy(strategy.seed());
          default -> throw new UnsupportedOperationException("Unknown strategy " + strategy.name());
        };

    Set<String> fooNames2 = new HashSet<>();
    Set<String> barNames2 = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      fooNames2.add(similar.generate("foo", i));
      barNames2.add(similar.generate("bar", i));
    }

    soft.assertThat(fooNames2).containsExactlyInAnyOrderElementsOf(fooNames);
    soft.assertThat(barNames2).containsExactlyInAnyOrderElementsOf(barNames);
  }

  static Stream<NamingStrategy> reuseNames() {
    return Stream.of(
        NamingStrategy.CONSTANT_PREFIX_NAME_GENERATOR, NamingStrategy.RANDOM_PREFIX_NAME_GENERATOR);
  }
}
