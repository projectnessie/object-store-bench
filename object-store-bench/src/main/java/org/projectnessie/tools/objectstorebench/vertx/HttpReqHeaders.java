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
package org.projectnessie.tools.objectstorebench.vertx;

import com.google.common.collect.AbstractIterator;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientRequest;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.tools.objectstorebench.signing.RequestSigner;

public class HttpReqHeaders implements RequestSigner.Headers {

  private final MultiMap headers;

  public HttpReqHeaders(HttpClientRequest request) {
    this.headers = request.headers();
  }

  public HttpReqHeaders(MultiMap headers) {
    this.headers = headers;
  }

  @Override
  public Optional<String> getFirst(String name) {
    return Optional.ofNullable(headers.get(name));
  }

  @Override
  public List<String> getAll(String name) {
    return headers.getAll(name);
  }

  @Override
  public RequestSigner.Headers put(String name, String value) {
    headers.add(name, value);
    return this;
  }

  @Override
  public Iterator<Map.Entry<String, List<String>>> iterator() {
    return new AbstractIterator<>() {
      final Iterator<String> names = headers.names().iterator();

      @Override
      protected Map.Entry<String, List<String>> computeNext() {
        if (!names.hasNext()) {
          return endOfData();
        }
        String name = names.next();
        return Map.entry(name, headers.getAll(name));
      }
    };
  }
}
