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

import static com.google.common.hash.Hashing.hmacSha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Collections.singletonList;
import static org.projectnessie.tools.objectstorebench.Utils.EMPTY_SHA256;
import static org.projectnessie.tools.objectstorebench.Utils.ISO8601_DATE_NO_Z_FORMATTER;
import static org.projectnessie.tools.objectstorebench.Utils.bytesAsHex;
import static org.projectnessie.tools.objectstorebench.Utils.formatIso8601DateTimeZulu;
import static org.projectnessie.tools.objectstorebench.Utils.parseIso8601DateTimeZulu;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.net.URI;
import java.net.URLEncoder;
import java.security.Key;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.crypto.spec.SecretKeySpec;

final class V4RequestSigner implements RequestSigner {
  private final String accessKey;
  private final SecretKeySpec aws4SecretKey;
  private final String region;
  private final String service;

  V4RequestSigner(SignerConfig signerConfig) {
    this.accessKey = signerConfig.accessKey();

    byte[] secretKeyBytes = ("AWS4" + signerConfig.secretKey()).getBytes(UTF_8);
    this.aws4SecretKey = new SecretKeySpec(secretKeyBytes, "hmacSha256");

    this.region = signerConfig.region();
    this.service = signerConfig.service();
  }

  @VisibleForTesting
  static final class V4SigningState {
    String scope;
    String dateTime;
    String canonicalRequestSha;
    String signedHeaders;
    TemporalAccessor timestamp;
    String timestampYMD;
  }

  @Override
  public ChunkSigningResult signChunk(ChunkSigningResult previousResult, byte[] chunk) {

    String hashOverChunk = Hashing.sha256().hashBytes(chunk).toString();

    String stringToSign =
        "AWS4-HMAC-SHA256-PAYLOAD\n"
            + previousResult.dateTime()
            + '\n'
            + previousResult.scope()
            + '\n'
            + previousResult.signature()
            + '\n'
            + EMPTY_SHA256
            + '\n'
            + hashOverChunk;

    String signature =
        bytesAsHex(
            hmacSha256(previousResult.signingKey()).hashString(stringToSign, UTF_8).asBytes());

    return ChunkSigningResult.builder().from(previousResult).signature(signature).build();
  }

  @Override
  public SigningResult signRequest(SigningRequest signingRequest) {
    V4SigningState state = new V4SigningState();
    SigningResult.Builder signingResult = SigningResult.builder();

    stringToSign(signingRequest, state, signingResult);

    // Calculate signature:
    // DateKey              = HMAC-SHA256("AWS4"+"<SecretAccessKey>", "<YYYYMMDD>")
    byte[] dateKey = hmacSha256(aws4SecretKey).hashString(state.timestampYMD, UTF_8).asBytes();
    // DateRegionKey        = hmacSha256-SHA256(<DateKey>, "<aws-region>")
    byte[] dateRegionKey = hmacSha256(dateKey).hashString(region, UTF_8).asBytes();
    // DateRegionServiceKey = HMAC-SHA256(<DateRegionKey>, "<aws-service>")
    byte[] dateRegionServiceKey = hmacSha256(dateRegionKey).hashString(service, UTF_8).asBytes();
    // SigningKey           = HMAC-SHA256(<DateRegionServiceKey>, "aws4_request")
    byte[] signingKeyBytes =
        hmacSha256(dateRegionServiceKey).hashString("aws4_request", UTF_8).asBytes();
    Key signingKey = new SecretKeySpec(signingKeyBytes, "HmacSHA256");

    String stringToSign =
        "AWS4-HMAC-SHA256\n"
            + state.dateTime
            + '\n'
            + state.scope
            + '\n'
            + state.canonicalRequestSha;
    String signature = bytesAsHex(hmacSha256(signingKey).hashString(stringToSign, UTF_8).asBytes());

    return signingResult
        .putHeadersToSet(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential="
                + accessKey
                + "/"
                + state.scope
                + ", SignedHeaders="
                + state.signedHeaders
                + ", Signature="
                + signature)
        .signature(signature)
        .canonicalRequestSha(state.canonicalRequestSha)
        .dateTime(state.dateTime)
        .scope(state.scope)
        .signingKey(signingKey)
        .build();
  }

  private void stringToSign(
      SigningRequest signingRequest, V4SigningState state, SigningResult.Builder signingResult) {
    /*
    From https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#create-signature-presign-entire-payload
    */

    StringBuilder canonicalRequest = canonicalRequest(signingRequest, state, signingResult);

    byte[] canonicalRequestSHA256 = Hashing.sha256().hashString(canonicalRequest, UTF_8).asBytes();
    String canonicalRequestSHA256hex = bytesAsHex(canonicalRequestSHA256);

    state.timestamp =
        signingRequest.timestamp().orElseGet(() -> extractTimestampFromHeaders(signingRequest));
    state.dateTime = formatIso8601DateTimeZulu(state.timestamp);
    state.timestampYMD = ISO8601_DATE_NO_Z_FORMATTER.format(state.timestamp);

    state.scope = state.timestampYMD + '/' + region + '/' + service + "/aws4_request";

    state.canonicalRequestSha = canonicalRequestSHA256hex;
  }

  private static TemporalAccessor extractTimestampFromHeaders(SigningRequest signingRequest) {
    Optional<String> timestampHeader = signingRequest.headers().getFirst("x-amz-date");
    if (timestampHeader.isPresent()) {
      return parseIso8601DateTimeZulu(timestampHeader.get());
    }

    // This fallback to the 'Date' header is rather esoteric.
    return signingRequest
        .headers()
        .getFirst("Date")
        .map(RFC_1123_DATE_TIME::parse)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Request must contain a X-AMZ-Date (preferred) or Date header"));
  }

  StringBuilder canonicalRequest(
      SigningRequest signingRequest, V4SigningState state, SigningResult.Builder signingResult) {
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

    StringBuilder canonicalRequest =
        new StringBuilder(signingRequest.httpMethodName()).append('\n');

    canonicalUri(canonicalRequest, signingRequest.uri()).append('\n');

    canonicalQuery(canonicalRequest, signingRequest.uri()).append('\n');

    String payloadSha256 =
        signingRequest.headers().getFirst("x-amz-content-sha256").orElse(EMPTY_SHA256);

    return canonicalHeaders(canonicalRequest, signingRequest, state, signingResult)
        .append('\n')
        .append(state.signedHeaders)
        .append('\n')
        .append(payloadSha256);
  }

  private static StringBuilder canonicalHeaders(
      StringBuilder target,
      SigningRequest signingRequest,
      V4SigningState state,
      SigningResult.Builder signingResult) {
    StringBuilder signedHeaders = new StringBuilder();

    List<Map.Entry<String, List<String>>> entries = headersSorted(signingRequest, signingResult);

    for (Map.Entry<String, List<String>> e : entries) {
      if (!signedHeaders.isEmpty()) {
        signedHeaders.append(';');
      }
      signedHeaders.append(e.getKey());

      // Functionally equivalent String.join(",", values), but not as expensive
      target.append(e.getKey()).append(':');
      List<String> values = e.getValue();
      for (int i = 0; i < values.size(); i++) {
        String v = values.get(i);
        if (i > 0) {
          target.append(',');
        }
        target.append(v);
      }
      target.append('\n');
    }

    state.signedHeaders = signedHeaders.toString();

    return target;
  }

  private static List<Map.Entry<String, List<String>>> headersSorted(
      SigningRequest signingRequest, SigningResult.Builder signingResult) {
    List<Map.Entry<String, List<String>>> entries = new ArrayList<>();
    for (Iterator<Map.Entry<String, List<String>>> headers = signingRequest.headers().iterator();
        headers.hasNext(); ) {
      Map.Entry<String, List<String>> header = headers.next();
      String lowerCaseName = header.getKey().toLowerCase(Locale.ROOT);
      switch (lowerCaseName) {
        case "user-agent":
        case "authorization":
        case "connection":
        case "expect":
          // ignore these headers
          continue;
        case "content-length":
          if (signingRequest.chunkLength().isPresent()) {
            String contentLength = header.getValue().get(0);
            long dataSize = Long.parseLong(contentLength);
            String realContentLength =
                Long.toString(
                    chunkedContentLength(dataSize, signingRequest.chunkLength().getAsInt()));
            signingResult.putHeadersToSet("x-amz-decoded-content-length", contentLength);
            signingResult.putHeadersToSet("content-length", realContentLength);
            entries.add(Map.entry("x-amz-decoded-content-length", singletonList(contentLength)));
            entries.add(Map.entry("content-length", singletonList(realContentLength)));
            break;
          }
          // fall-through
        default:
          entries.add(Map.entry(lowerCaseName, header.getValue()));
      }
    }
    entries.sort(Map.Entry.comparingByKey());
    return entries;
  }

  static Stream<Map.Entry<String, List<String>>> queryEntriesSorted(String q) {
    if (q == null) {
      return Stream.empty();
    }

    Map<String, List<String>> params = new HashMap<>();

    int n;
    int l = q.length();
    for (int p = 0; p < l; p = n) {
      int amp = q.indexOf('&', p);
      int e;
      if (amp != -1) {
        n = (e = amp) + 1;
      } else {
        n = e = l;
      }

      int eq = q.indexOf('=', p);
      String k;
      String v;
      if (eq == -1 || eq > e) {
        k = q.substring(p, e);
        v = null;
      } else {
        k = q.substring(p, eq);
        v = q.substring(eq + 1, e);
      }
      String keyLowerCase = k.toLowerCase(Locale.ROOT);
      List<String> values = params.computeIfAbsent(keyLowerCase, x -> new ArrayList<>());
      if (v != null) {
        values.add(v);
      }
    }

    return params.entrySet().stream().sorted(Map.Entry.comparingByKey());
  }

  static StringBuilder canonicalQuery(StringBuilder target, URI uri) {
    String rawQuery = uri.getRawQuery();
    if (rawQuery == null) {
      return target;
    }

    boolean first = true;
    for (Iterator<Map.Entry<String, List<String>>> params = queryEntriesSorted(rawQuery).iterator();
        params.hasNext(); ) {
      Map.Entry<String, List<String>> e = params.next();
      if (first) {
        first = false;
      } else {
        target.append('&');
      }
      List<String> values = e.getValue();
      if (values.isEmpty()) {
        target.append(e.getKey()).append('=');
      } else {
        for (int i = 0; i < values.size(); i++) {
          String v = values.get(i);
          if (i > 0) {
            target.append('&');
          }
          target.append(e.getKey()).append('=').append(v);
        }
      }
    }
    return target;
  }

  /** URL-encode, but not {@code /}. */
  private static StringBuilder canonicalUri(StringBuilder target, URI uri) {
    String path = uri.getRawPath();
    // TODO find a better implementation that can handle '/' and append to 'target' directly and
    //  avoids the '.substring()'
    for (int p = 0; p < path.length(); ) {
      int idx = path.indexOf('/', p);
      if (idx == -1) {
        target.append(URLEncoder.encode(path.substring(p), UTF_8));
        break;
      } else {
        target.append(URLEncoder.encode(path.substring(p, idx), UTF_8)).append('/');
        p = idx + 1;
      }
    }
    return target;
  }

  static final int CHUNK_CONSTANT_PART_LEN =
      // hex-representation of chunk length goes here
      ";chunk-signature=".length()
          + EMPTY_SHA256.length()
          + "\r\n".length()
          // chunk data goes here
          + "\r\n".length();

  static long chunkedContentLength(long dataLength, int chunkLength) {
    // Chunk format:
    //   %x;chunk-signature=%s\r\n
    //   <chunk data>
    //   \r\n

    int chunkLenHexLen = hexStringLength(chunkLength);
    int fullChunkHeaderLength = chunkLenHexLen + CHUNK_CONSTANT_PART_LEN;
    long numFullChunks = dataLength / chunkLength;

    long contentLength = dataLength;
    contentLength += numFullChunks * fullChunkHeaderLength;

    int partChunkLength = (int) (dataLength % chunkLength);
    if (partChunkLength > 0) {
      int partChunkHeaderLength = hexStringLength(partChunkLength) + CHUNK_CONSTANT_PART_LEN;
      contentLength += partChunkHeaderLength;
    }

    // 0-length trailing chunk
    contentLength += 1 + CHUNK_CONSTANT_PART_LEN;

    return contentLength;
  }

  static int hexStringLength(int n) {
    if (n == 0) {
      return 1;
    }

    int bitLength = 32 - Integer.numberOfLeadingZeros(n);
    int len = (bitLength & 3) != 0 ? 1 : 0;
    len += bitLength >> 2;
    return len;
  }
}
