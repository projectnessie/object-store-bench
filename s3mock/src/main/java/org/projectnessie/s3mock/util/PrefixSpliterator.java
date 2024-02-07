/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.s3mock.util;

import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.projectnessie.s3mock.S3Resource;

/**
 * {@link Spliterator} used when listing objects in a bucket, see {@link
 * S3Resource#listObjectsInsideBucket(String, String, String, String, String, int, int, String,
 * String, String)}.
 */
public final class PrefixSpliterator<T> extends AbstractSpliterator<T> {

  private final Spliterator<T> source;
  private final Predicate<T> matcher;
  private boolean didMatch;
  private boolean exhausted;
  private T value;

  public PrefixSpliterator(Spliterator<T> source, Predicate<T> matcher) {
    super(source.estimateSize(), source.characteristics());
    this.source = source;
    this.matcher = matcher;
  }

  private void set(T value) {
    this.value = value;
  }

  @Override
  public boolean tryAdvance(Consumer<? super T> action) {
    while (true) {
      if (exhausted) {
        return false;
      }

      if (!source.tryAdvance(this::set)) {
        exhausted = true;
        return false;
      }

      boolean currentMatches = matcher.test(value);

      if (currentMatches) {
        action.accept(value);
      }

      if (didMatch) {
        if (!currentMatches) {
          exhausted = true;
          return false;
        }
      } else {
        if (currentMatches) {
          didMatch = true;
        }
      }

      if (currentMatches) {
        return true;
      }
    }
  }
}
