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

import org.immutables.value.Value;

@Value.Immutable
public interface SignerConfig {
  String accessKey();

  String secretKey();

  String region();

  String service();

  static Builder builder() {
    return ImmutableSignerConfig.builder();
  }

  interface Builder {
    Builder from(SignerConfig source);

    Builder accessKey(String accessKey);

    Builder secretKey(String secretKey);

    Builder region(String region);

    Builder service(String service);

    SignerConfig build();
  }
}
