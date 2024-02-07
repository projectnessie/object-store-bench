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
package org.projectnessie.tools.objectstorebench.signing;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.security.Key;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public abstract class SigningResult {
  public abstract String signature();

  public abstract String canonicalRequestSha();

  public abstract String dateTime();

  public abstract String scope();

  public abstract Map<String, String> headersToSet();

  @Nullable
  @Value.Auxiliary
  abstract Key signingKey();

  static Builder builder() {
    return ImmutableSigningResult.builder();
  }

  public ChunkSigningResult asChunkSigningResult() {
    return ChunkSigningResult.builder()
        .signingKey(signingKey())
        .dateTime(dateTime())
        .scope(scope())
        .signature(signature())
        .build();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder from(SigningResult source);

    @CanIgnoreReturnValue
    Builder signature(String signature);

    @CanIgnoreReturnValue
    Builder canonicalRequestSha(String canonicalRequestSha);

    @CanIgnoreReturnValue
    Builder dateTime(String dateTime);

    @CanIgnoreReturnValue
    Builder scope(String scope);

    @CanIgnoreReturnValue
    Builder signingKey(Key signingKey);

    @CanIgnoreReturnValue
    Builder putHeadersToSet(String key, String value);

    SigningResult build();
  }
}
