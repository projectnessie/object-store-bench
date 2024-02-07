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

import static java.util.Locale.ROOT;

import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.CompletionStage;

public interface GetPut extends AutoCloseable {
  String name();

  String region();

  URI baseUri();

  boolean forcePathStyle();

  CompletionStage<RequestStats> doGet(String object);

  CompletionStage<RequestStats> doPut(String object, InputStream data, long dataSize);

  CompletionStage<RequestStats> doDelete(String object);

  default String region(GetPutOpts opts) {
    return opts.s3.region.orElseThrow(() -> new IllegalArgumentException("No region specified"));
  }

  default URI baseUri(GetPutOpts opts) {
    return URI.create(
        opts.baseUri.orElseGet(
            () -> {
              String cloud = opts.s3.cloud.orElse("aws").toLowerCase(ROOT);
              return switch (cloud) {
                case "aws", "amazon" -> amazonBaseUri(opts);
                case "gcp", "gcs", "google" -> "https://storage.googleapis.com/";
                default ->
                    throw new IllegalArgumentException(
                        "Cannot infer base-URI for cloud '" + cloud + "'");
              };
            }));
  }

  default String amazonBaseUri(GetPutOpts opts) {
    return String.format("https://s3.%s.amazonaws.com/", region(opts));
  }
}
