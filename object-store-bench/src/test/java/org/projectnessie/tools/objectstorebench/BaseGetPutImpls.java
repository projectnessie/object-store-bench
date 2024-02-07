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
package org.projectnessie.tools.objectstorebench;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public abstract class BaseGetPutImpls extends BaseS3MockServer {
  protected abstract GetPut buildGetPut(GetPutOpts opts);

  @Test
  public void getPut() throws Exception {
    try (GetPut getPut = buildGetPut(opts)) {
      getPut(getPut);
    }
  }

  protected void getPut(GetPut getPut)
      throws InterruptedException, ExecutionException, TimeoutException {
    byte[] myData = new byte[1024 * 1024];
    RequestStats put =
        getPut
            .doPut("my/object", new ByteArrayInputStream(myData), myData.length)
            .toCompletableFuture()
            .get(10, SECONDS);
    soft.assertThat(put)
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.of(myData.length), Optional.empty(), 200);

    RequestStats get = getPut.doGet("my/object").toCompletableFuture().get(10, SECONDS);
    soft.assertThat(get)
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.of(myData.length), Optional.empty(), 200);

    @SuppressWarnings("unchecked")
    CompletableFuture<RequestStats>[] futures =
        IntStream.range(0, 3)
            .mapToObj(x -> getPut.doGet("my/object").toCompletableFuture())
            .toArray(CompletableFuture[]::new);
    CompletableFuture<Void> combined = CompletableFuture.allOf(futures);
    combined.get(10, SECONDS);
    soft.assertThat(futures)
        .allMatch(
            cf -> {
              try {
                return cf.get(0, MINUTES).status() == 200;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    soft.assertThat(getPut.doDelete("my/object").toCompletableFuture().get(10, SECONDS))
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.empty(), Optional.empty(), 200);
  }
}
