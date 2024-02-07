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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class BaseGetPutIT {

  protected void getPutImpl(GetPut getPut) throws Exception {
    byte[] data = new byte[1024 * 1024 * 100];
    ThreadLocalRandom.current().nextBytes(data);

    String object = "my-object-" + ThreadLocalRandom.current().nextLong(1000000, Long.MAX_VALUE);

    assertThat(
            getPut
                .doPut(object, new ByteArrayInputStream(data), data.length)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS))
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.of(data.length), Optional.empty(), 200);

    assertThat(getPut.doGet(object).toCompletableFuture().get(10, TimeUnit.SECONDS))
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.of(data.length), Optional.empty(), 200);

    CompletableFuture<Void> combined =
        CompletableFuture.allOf(
            IntStream.range(0, 100)
                .mapToObj(x -> getPut.doGet(object).toCompletableFuture())
                .toArray(CompletableFuture[]::new));
    combined.get(10, MINUTES);

    assertThat(getPut.doDelete(object).toCompletableFuture().get(10, TimeUnit.SECONDS))
        .extracting(RequestStats::contentLength, RequestStats::failure, RequestStats::status)
        .containsExactly(OptionalLong.empty(), Optional.empty(), 200);
  }
}
