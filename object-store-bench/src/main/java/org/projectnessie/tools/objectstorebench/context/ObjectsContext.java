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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class ObjectsContext {
  private final String name;
  private final List<String> objects = new ArrayList<>();
  private final NamingStrategy namingStrategy;
  private int num;

  public ObjectsContext(String name, NamingStrategy namingStrategy) {
    this.name = name;
    this.namingStrategy = namingStrategy;
  }

  public NamingStrategy namingStrategy() {
    return namingStrategy;
  }

  public String create() {
    synchronized (this) {
      String object = namingStrategy.generate(name, num++);
      objects.add(object);
      return object;
    }
  }

  public String get(int i) {
    synchronized (this) {
      return objects.get(i);
    }
  }

  public String getRandom() {
    synchronized (this) {
      return get(ThreadLocalRandom.current().nextInt(objects.size()));
    }
  }

  public Stream<String> all() {
    return objects.stream();
  }

  public String removeLast() {
    synchronized (this) {
      int size = objects.size();
      if (size == 0) {
        return null;
      }
      return objects.remove(size - 1);
    }
  }

  public String removeRandom() {
    synchronized (this) {
      int size = objects.size();
      if (size == 0) {
        return null;
      }
      return objects.remove(ThreadLocalRandom.current().nextInt(size));
    }
  }

  public boolean isEmpty() {
    synchronized (this) {
      return objects.isEmpty();
    }
  }
}
