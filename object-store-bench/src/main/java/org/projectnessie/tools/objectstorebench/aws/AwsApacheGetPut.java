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
package org.projectnessie.tools.objectstorebench.aws;

import org.projectnessie.tools.objectstorebench.GetPutOpts;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public class AwsApacheGetPut extends AwsGetPut {
  public AwsApacheGetPut(GetPutOpts opts) {
    super(opts, "Apache", httpClientBuilder(opts));
  }

  private static ApacheHttpClient.Builder httpClientBuilder(GetPutOpts opts) {
    ApacheHttpClient.Builder b = ApacheHttpClient.builder();
    opts.http.maxConnections.ifPresent(b::maxConnections);
    opts.http.readTimeout.ifPresent(b::socketTimeout);
    opts.http.connectTimeout.ifPresent(b::connectionTimeout);
    opts.http.connectionAcquisitionTimeout.ifPresent(b::connectionAcquisitionTimeout);
    opts.http.expectContinueEnabled.ifPresent(b::expectContinueEnabled);
    return b;
  }
}
