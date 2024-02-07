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

import java.net.URI;
import org.projectnessie.tools.objectstorebench.time.MonotonicClock;

public abstract class AbstractGetPut implements GetPut {
  protected final URI baseUri;
  protected final String region;
  protected final boolean forcePathStyle;
  protected final MonotonicClock clock;

  protected AbstractGetPut(GetPutOpts opts) {
    String amzBaseUri = amazonBaseUri(opts);
    URI baseUri = baseUri(opts);
    this.forcePathStyle = !amzBaseUri.equals(baseUri.toString());
    this.baseUri = forcePathStyle ? baseUri : null;
    this.region = opts.s3.region.orElseThrow();
    this.clock = MonotonicClock.newMonotonicClock();
  }

  @Override
  public String region() {
    return region;
  }

  @Override
  public URI baseUri() {
    return baseUri;
  }

  @Override
  public boolean forcePathStyle() {
    return forcePathStyle;
  }

  @Override
  public void close() throws Exception {
    clock.close();
  }
}
