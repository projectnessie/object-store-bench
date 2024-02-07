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
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

public class AwsNettyGetPut extends AwsAsyncGetPut {
  public AwsNettyGetPut(GetPutOpts opts) {
    super(opts, "Netty", httpClientBuilder(opts));
  }

  private static NettyNioAsyncHttpClient.Builder httpClientBuilder(GetPutOpts opts) {
    NettyNioAsyncHttpClient.Builder b = NettyNioAsyncHttpClient.builder();
    opts.http.connectTimeout.ifPresent(b::connectionTimeout);
    opts.http.readTimeout.ifPresent(b::readTimeout);
    opts.http.writeTimeout.ifPresent(b::writeTimeout);
    opts.http.connectionAcquisitionTimeout.ifPresent(b::connectionAcquisitionTimeout);
    opts.http.maxConcurrency.ifPresent(b::maxConcurrency);
    opts.http.protocol.ifPresent(b::protocol);
    opts.http.maxHttp2Streams.ifPresent(
        max -> b.http2Configuration(Http2Configuration.builder().maxStreams(max).build()));
    return b;
  }
}
