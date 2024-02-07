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
import java.net.URI;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
public interface SigningRequest {
  String httpMethodName();

  @Nullable
  String bucket();

  RequestSigner.Headers headers();

  boolean forcePathStyle();

  URI uri();

  @Nullable
  String subresource();

  Optional<TemporalAccessor> timestamp();

  OptionalInt chunkLength();

  static Builder builder() {
    return ImmutableSigningRequest.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder httpMethodName(String httpMethodName);

    @CanIgnoreReturnValue
    Builder bucket(String bucket);

    @CanIgnoreReturnValue
    Builder headers(RequestSigner.Headers headers);

    @CanIgnoreReturnValue
    Builder forcePathStyle(boolean forcePathStyle);

    @CanIgnoreReturnValue
    Builder uri(URI uri);

    @CanIgnoreReturnValue
    Builder subresource(String subresource);

    @CanIgnoreReturnValue
    Builder timestamp(TemporalAccessor timestamp);

    @CanIgnoreReturnValue
    Builder chunkLength(int chunkLength);

    SigningRequest build();
  }
}
