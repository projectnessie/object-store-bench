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

import java.util.function.Function;

public enum SignatureVersion {
  /**
   * Signature Version 2 algorithm as described <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html">here for
   * REST requests</a>. This version is outdated and practically no implementation supports it,
   * except Minio, if explicitly instructed to support it.
   */
  @Deprecated
  V2(V2RequestSigner::new),
  /**
   * Signature Version 4 algorithm as described <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html">here</a>.
   * This version should be used.
   */
  V4(V4RequestSigner::new),
  ;

  private final Function<SignerConfig, RequestSigner> builder;

  SignatureVersion(Function<SignerConfig, RequestSigner> builder) {
    this.builder = builder;
  }

  public RequestSigner newSigner(SignerConfig signerConfig) {
    return builder.apply(signerConfig);
  }
}
