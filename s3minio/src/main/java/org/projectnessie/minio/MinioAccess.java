/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.minio;

import java.net.URI;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Provides access to Minio via a preconfigured S3 client and providing the by default randomized
 * bucket and access/secret keys.
 *
 * <p>Annotate JUnit test instance or static fields or method parameters of this type with {@link
 * Minio}.
 */
public interface MinioAccess {

  /** Host and port, separated by '{@code :}'. */
  String hostPort();

  String accessKey();

  String secretKey();

  String bucket();

  /** HTTP protocol endpoint. */
  String s3endpoint();

  S3Client s3Client();

  /** S3 scheme URI including the bucket to access the given path. */
  URI s3BucketUri(String path);

  /** Convenience method to put an object into S3. */
  @SuppressWarnings("resource")
  default void s3put(String key, RequestBody body) {
    s3Client().putObject(b -> b.bucket(bucket()).key(key), body);
  }
}
