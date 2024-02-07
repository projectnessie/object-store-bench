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

import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Locale.ROOT;
import static java.util.Map.Entry.comparingByKey;

import com.google.common.hash.Hashing;
import java.security.Key;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.crypto.spec.SecretKeySpec;

final class V2RequestSigner implements RequestSigner {
  private final String accessKey;
  private final Key secretKey;

  V2RequestSigner(SignerConfig signerConfig) {
    this.accessKey = signerConfig.accessKey();
    byte[] secretKeyBytes = signerConfig.secretKey().getBytes(UTF_8);
    this.secretKey = new SecretKeySpec(secretKeyBytes, "hmacSha256");
  }

  @Override
  public SigningResult signRequest(SigningRequest signingRequest) {
    String stringToSign = stringToSign(signingRequest);

    byte[] sha1 = Hashing.hmacSha1(secretKey).hashString(stringToSign, UTF_8).asBytes();
    String sha1base64 = Base64.getEncoder().encodeToString(sha1);

    return SigningResult.builder()
        .putHeadersToSet("Authorization", "AWS " + accessKey + ':' + sha1base64)
        .signature(sha1base64)
        .canonicalRequestSha("")
        .scope("")
        .dateTime("")
        .build();
  }

  @Override
  public ChunkSigningResult signChunk(ChunkSigningResult previousResult, byte[] chunk) {
    throw new UnsupportedOperationException();
  }

  private String stringToSign(SigningRequest signingRequest) {
    /*
    From https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html#ConstructingTheAuthenticationHeader

    Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;

    Signature = Base64( HMAC-SHA1( UTF-8-Encoding-Of(YourSecretAccessKey), UTF-8-Encoding-Of( StringToSign ) ) );

    StringToSign = HTTP-Verb + "\n" +
      Content-MD5 + "\n" +
      Content-Type + "\n" +
      Date + "\n" +
      CanonicalizedAmzHeaders +
      CanonicalizedResource;

    CanonicalizedResource = [ "/" + Bucket ] +
      <HTTP-Request-URI, from the protocol name up to the query string> +
      [ subresource, if present. For example "?acl", "?location", or "?logging"];

    CanonicalizedAmzHeaders = <described below>
    */

    StringBuilder stringToSign = new StringBuilder(signingRequest.httpMethodName()).append('\n');
    signingRequest.headers().getFirst("Content-MD5").ifPresent(stringToSign::append);
    stringToSign.append('\n');

    signingRequest.headers().getFirst("Content-Type").ifPresent(stringToSign::append);
    stringToSign.append('\n');

    // Date + X-AMZ-Date
    Optional<String> amzDate = signingRequest.headers().getFirst("x-amz-date");
    Optional<String> date = signingRequest.headers().getFirst("Date");
    stringToSign.append(amzDate.orElseGet(() -> date.orElseGet(this::dateValue))).append('\n');

    // CanonicalizedAmzHeaders
    StreamSupport.stream(signingRequest.headers().spliterator(), false)
        .filter(
            e -> {
              String k = e.getKey().toLowerCase(ROOT);
              return k.startsWith("x-amz-") && !"x-amz-date".equals(k);
            })
        .map(e -> Map.entry(e.getKey().toLowerCase(ROOT), join(",", e.getValue())))
        .sorted(comparingByKey())
        .forEach(
            e -> stringToSign.append(e.getKey()).append(':').append(e.getValue()).append('\n'));

    // CanonicalizedResource
    if (signingRequest.bucket() == null) {
      // Not a bucket / S3 request
      stringToSign.append("/").append(signingRequest.uri().getHost());
    } else {
      if (!signingRequest.forcePathStyle() && !signingRequest.bucket().isEmpty()) {
        // virtual hosted-style request / bucket-name encoded in host name, not in the URI path
        stringToSign.append("/").append(signingRequest.bucket());
      }
    }
    stringToSign.append(signingRequest.uri().getRawPath());
    if (signingRequest.subresource() != null) {
      stringToSign.append('?').append(signingRequest.subresource());
    }
    // no newline after CanonicalizedResource!
    return stringToSign.toString();
  }

  String dateValue() {
    return RFC_1123_DATE_TIME.format(Instant.now().atZone(ZoneId.systemDefault()));
  }
}
