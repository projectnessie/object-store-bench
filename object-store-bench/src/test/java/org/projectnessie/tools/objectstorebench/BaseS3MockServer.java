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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.s3mock.IcebergS3Mock;
import org.projectnessie.s3mock.MockObject;
import org.projectnessie.s3mock.ObjectRetriever;
import org.projectnessie.s3mock.S3Bucket;
import org.projectnessie.s3mock.S3Bucket.PutObject;
import software.amazon.awssdk.regions.Region;

public abstract class BaseS3MockServer {
  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @AfterEach
  void softAssert() {
    soft.assertAll();
  }

  protected static IcebergS3Mock.S3MockServer server;
  protected static final String bucket = "bucket";
  protected static GetPutOpts opts;

  @BeforeAll
  static void setup() {
    Map<String, byte[]> objects = new ConcurrentHashMap<>();
    PutObject putObject = (key1, contentType, value) -> objects.put(key1, value);
    ObjectRetriever objectRetriever =
        key -> {
          byte[] data = objects.get(key);
          return data != null
              ? MockObject.builder()
                  .contentLength(data.length)
                  .lastModified(System.currentTimeMillis())
                  .etag("hello")
                  .writer(new MockObject.RangeAwareBytesWriter(data))
                  .contentType("application/octet-stream")
                  .build()
              : null;
        };

    IcebergS3Mock s3mock =
        IcebergS3Mock.builder()
            .putBuckets(
                bucket,
                S3Bucket.builder()
                    .object(objectRetriever)
                    .deleter(oid -> true)
                    .lister(prefix -> Stream.empty())
                    .putObject(putObject)
                    .build())
            .build();

    server = s3mock.start();

    URI base = server.getBaseUri();
    opts = new GetPutOpts();
    opts.baseUri = Optional.of(base.toString());
    opts.s3.region = Optional.of(Region.EU_CENTRAL_1.id());
    opts.s3.accessKey = "access";
    opts.s3.secretKey = "secret";
  }

  @BeforeAll
  static void tearDown() throws Exception {
    if (server != null) {
      try {
        server.close();
      } finally {
        server = null;
      }
    }
  }
}
