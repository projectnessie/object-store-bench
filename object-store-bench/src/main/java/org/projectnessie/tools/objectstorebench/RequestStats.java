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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.OptionalLong;
import org.immutables.value.Value;

@Value.Immutable
public interface RequestStats {
  long started();

  Optional<Throwable> failure();

  @Value.Default
  default int status() {
    return 0;
  }

  Optional<String> failureMessage();

  OptionalLong contentLength();

  long durationMicros();

  OptionalLong firstByteMicros();

  static Builder builder() {
    return ImmutableRequestStats.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder started(long started);

    @CanIgnoreReturnValue
    Builder firstByteMicros(long firstByteMicros);

    @CanIgnoreReturnValue
    Builder durationMicros(long durationMicros);

    @CanIgnoreReturnValue
    Builder failureMessage(String failureMessage);

    @CanIgnoreReturnValue
    Builder failure(Throwable failure);

    @CanIgnoreReturnValue
    Builder status(int status);

    @CanIgnoreReturnValue
    Builder contentLength(long contentLength);

    RequestStats build();
  }

  class TimeToFirstByteInputStream extends InputStream {
    private final InputStream delegate;
    private final Runnable firstByte;
    private boolean first = true;

    public TimeToFirstByteInputStream(InputStream delegate, Runnable firstByte) {
      this.delegate = delegate;
      this.firstByte = firstByte;
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public int read() throws IOException {
      int r = delegate.read();
      if (r >= 0 && first) {
        first = false;
        firstByte.run();
      }
      return r;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int r = delegate.read(b, off, len);
      if (r > 0 && first) {
        first = false;
        firstByte.run();
      }
      return r;
    }

    @Override
    public long skip(long n) throws IOException {
      long r = delegate.skip(n);
      if (r > 0 && first) {
        first = false;
        firstByte.run();
      }
      return r;
    }
  }
}
