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

import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class BaseS3GetPutImpls extends BaseGetPutImpls {
  @Test
  public void usesNoPathStyleForAws() throws Exception {
    GetPutOpts opts = new GetPutOpts();
    opts.s3.cloud = Optional.of("aws");
    opts.s3.region = Optional.of("arctic-middle-42");
    try (GetPut getPut = buildGetPut(opts)) {
      soft.assertThat(getPut.baseUri()).isNull();
      soft.assertThat(getPut.forcePathStyle()).isFalse();
      soft.assertThat(getPut.region()).isEqualTo(opts.s3.region.get());
    }
  }

  @Test
  public void usesPathStyleForGcsS3() throws Exception {
    GetPutOpts opts = new GetPutOpts();
    opts.s3.cloud = Optional.of("gcp");
    opts.s3.region = Optional.of("arctic-middle-42");
    try (GetPut getPut = buildGetPut(opts)) {
      soft.assertThat(getPut.baseUri().toString()).isEqualTo("https://storage.googleapis.com/");
      soft.assertThat(getPut.forcePathStyle()).isTrue();
      soft.assertThat(getPut.region()).isEqualTo(opts.s3.region.get());
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void usesPathStyleForOtherCloud() throws Exception {
    GetPutOpts opts = new GetPutOpts();
    opts.s3.cloud = Optional.of("my-own-minio");
    opts.s3.region = Optional.of("playground");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> buildGetPut(opts))
        .withMessage("Cannot infer base-URI for cloud 'my-own-minio'");
    opts.baseUri = Optional.of("https://localhost:4242/");
    try (GetPut getPut = buildGetPut(opts)) {
      soft.assertThat(getPut.baseUri().toString()).isEqualTo("https://localhost:4242/");
      soft.assertThat(getPut.forcePathStyle()).isTrue();
      soft.assertThat(getPut.region()).isEqualTo(opts.s3.region.get());
    }
  }
}
