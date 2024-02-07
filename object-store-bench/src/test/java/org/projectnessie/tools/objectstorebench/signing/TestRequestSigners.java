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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.tools.objectstorebench.Utils.EMPTY_SHA256;

import com.google.common.collect.ImmutableList;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.objectstorebench.vertx.HttpReqHeaders;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRequestSigners {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("v2")
  public void signV2(SigningRequest signingRequest, SigningResult expectedResult) {
    RequestSigner signer =
        new V2RequestSigner(
            SignerConfig.builder()
                .accessKey("AKIAIOSFODNN7EXAMPLE")
                .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .region("us-east-1")
                .service("s3")
                .build());

    sign(signingRequest, expectedResult, signer);
  }

  @ParameterizedTest
  @MethodSource("v4")
  public void signV4(
      SigningRequest signingRequest,
      SigningResult expectedResult,
      String expectedCanonicalRequest) {
    V4RequestSigner signer =
        new V4RequestSigner(
            SignerConfig.builder()
                .accessKey("AKIAIOSFODNN7EXAMPLE")
                .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .region("us-east-1")
                .service("s3")
                .build());

    V4RequestSigner.V4SigningState state = new V4RequestSigner.V4SigningState();
    SigningResult.Builder signingResult = SigningResult.builder();
    StringBuilder canonicalRequest = signer.canonicalRequest(signingRequest, state, signingResult);
    soft.assertThat(canonicalRequest.toString()).isEqualTo(expectedCanonicalRequest);

    SigningResult result = sign(signingRequest, expectedResult, signer);
    soft.assertThat(result.signingKey()).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("v4chunked")
  public void signV4chunked(
      SigningRequest signingRequest,
      SigningResult expectedResult,
      String expectedCanonicalRequest,
      List<String> expectedChunkSignatures) {
    V4RequestSigner signer =
        new V4RequestSigner(
            SignerConfig.builder()
                .accessKey("AKIAIOSFODNN7EXAMPLE")
                .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .region("us-east-1")
                .service("s3")
                .build());

    V4RequestSigner.V4SigningState state = new V4RequestSigner.V4SigningState();
    SigningResult.Builder signingResult = SigningResult.builder();
    StringBuilder canonicalRequest = signer.canonicalRequest(signingRequest, state, signingResult);
    soft.assertThat(canonicalRequest.toString()).isEqualTo(expectedCanonicalRequest);

    SigningResult result = signer.signRequest(signingRequest);
    soft.assertThat(result).isEqualTo(expectedResult);
    soft.assertThat(result.signingKey()).isNotNull();

    int chunkSize = 65536;
    int dataLength =
        Integer.parseInt(
            signingRequest.headers().getFirst("x-amz-decoded-content-length").orElseThrow());
    List<String> chunkSignatures = new ArrayList<>();
    ChunkSigningResult chunkResult = result.asChunkSigningResult();
    while (dataLength > 0) {
      int len = Math.min(dataLength, chunkSize);
      dataLength -= len;

      byte[] chunk = new byte[len];
      Arrays.fill(chunk, (byte) 'a');

      chunkResult = signer.signChunk(chunkResult, chunk);
      chunkSignatures.add(chunkResult.signature());
    }
    chunkResult = signer.signChunk(chunkResult, new byte[0]);
    chunkSignatures.add(chunkResult.signature());

    soft.assertThat(chunkSignatures).containsExactlyElementsOf(expectedChunkSignatures);
  }

  @ParameterizedTest
  @CsvSource({
    "0,65536",
    "1,65536",
    "65535,65536",
    "65536,65536",
    "65537,65536",
    "1048575,65536",
    "1048576,65536",
    "1048577,65536",
    "104857600,65536",
  })
  public void v4chunkedContentLength(long dataLength, int chunkLength) {
    long expectedContentLength = dataLength;
    long workDataLength = dataLength;
    while (true) {
      if (workDataLength >= chunkLength) {
        expectedContentLength +=
            String.format("%x;chunk-signature=%s\r\n\r\n", chunkLength, EMPTY_SHA256).length();
        workDataLength -= chunkLength;
      } else {
        if (workDataLength > 0) {
          expectedContentLength +=
              String.format("%x;chunk-signature=%s\r\n\r\n", workDataLength, EMPTY_SHA256).length();
        }
        expectedContentLength +=
            String.format("%x;chunk-signature=%s\r\n\r\n", 0, EMPTY_SHA256).length();
        break;
      }
    }

    soft.assertThat(V4RequestSigner.chunkedContentLength(dataLength, chunkLength))
        .isEqualTo(expectedContentLength);
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        0,
        1,
        3,
        4,
        15,
        16,
        0xffff,
        0x10000,
        0x10001,
        0xfffffff,
        0x10000000,
        0x7fffffff,
        0x80000000
      })
  public void v4hexStringLength(int n) {
    soft.assertThat(V4RequestSigner.hexStringLength(n)).isEqualTo(Integer.toHexString(n).length());
  }

  private SigningResult sign(
      SigningRequest signingRequest, SigningResult expectedResult, RequestSigner signer) {
    SigningResult result = signer.signRequest(signingRequest);
    soft.assertThat(result).isEqualTo(expectedResult);
    return result;
  }

  static Stream<Arguments> v4chunked() {
    return Stream.of(
        // Canonical request example from
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
        arguments(
            SigningRequest.builder()
                .httpMethodName("PUT")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "s3.amazonaws.com")
                                    .add("x-amz-date", "20130524T000000Z")
                                    .add("x-amz-storage-class", "REDUCED_REDUNDANCY")
                                    .add(
                                        "x-amz-content-sha256",
                                        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                                    .add("Content-Encoding", "aws-chunked")
                                    .add("x-amz-decoded-content-length", "66560")
                                    .add("Content-Length", "66824"))))
                .bucket("examplebucket")
                .forcePathStyle(false)
                .uri(URI.create("https://s3.amazonaws.com/examplebucket/chunkObject.txt"))
                .build(),
            SigningResult.builder()
                .putHeadersToSet(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class, Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
                .signature("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
                .dateTime("20130524T000000Z")
                .scope("20130524/us-east-1/s3/aws4_request")
                .canonicalRequestSha(
                    "cee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455")
                .build(),
            "PUT\n"
                + "/examplebucket/chunkObject.txt\n"
                + "\n"
                + "content-encoding:aws-chunked\n"
                + "content-length:66824\n"
                + "host:s3.amazonaws.com\n"
                + "x-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD\n"
                + "x-amz-date:20130524T000000Z\n"
                + "x-amz-decoded-content-length:66560\n"
                + "x-amz-storage-class:REDUCED_REDUNDANCY\n"
                + "\n"
                + "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class\n"
                + "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            asList(
                "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
                "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
                "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9")));
  }

  static Stream<Arguments> v4() {
    // test cases taken from
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    return Stream.of(
        // Example: GET Object
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "examplebucket.s3.amazonaws.com")
                                    .add("Range", "bytes=0-9")
                                    .add(
                                        "x-amz-content-sha256",
                                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                                    .add("x-amz-date", "20130524T000000Z"))))
                .bucket("examplebucket")
                .forcePathStyle(false)
                .uri(URI.create("https://examplebucket.s3.amazonaws.com/test.txt"))
                .build(),
            SigningResult.builder()
                .putHeadersToSet(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
                .signature("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
                .dateTime("20130524T000000Z")
                .scope("20130524/us-east-1/s3/aws4_request")
                .canonicalRequestSha(
                    "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972")
                .build(),
            "GET\n"
                + "/test.txt\n"
                + "\n"
                + "host:examplebucket.s3.amazonaws.com\n"
                + "range:bytes=0-9\n"
                + "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                + "x-amz-date:20130524T000000Z\n"
                + "\n"
                + "host;range;x-amz-content-sha256;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        // "Example: PUT Object
        arguments(
            SigningRequest.builder()
                .httpMethodName("PUT")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "examplebucket.s3.amazonaws.com")
                                    .add("Date", "Fri, 24 May 2013 00:00:00 GMT")
                                    .add("x-amz-storage-class", "REDUCED_REDUNDANCY")
                                    .add(
                                        "x-amz-content-sha256",
                                        "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
                                    .add("x-amz-date", "20130524T000000Z"))))
                .bucket("examplebucket")
                .forcePathStyle(false)
                .uri(URI.create("https://examplebucket.s3.amazonaws.com/test$file.text"))
                .build(),
            SigningResult.builder()
                .putHeadersToSet(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")
                .signature("98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")
                .dateTime("20130524T000000Z")
                .scope("20130524/us-east-1/s3/aws4_request")
                .canonicalRequestSha(
                    "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d")
                .build(),
            "PUT\n"
                + "/test%24file.text\n"
                + "\n"
                + "date:Fri, 24 May 2013 00:00:00 GMT\n"
                + "host:examplebucket.s3.amazonaws.com\n"
                + "x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n"
                + "x-amz-date:20130524T000000Z\n"
                + "x-amz-storage-class:REDUCED_REDUNDANCY\n"
                + "\n"
                + "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n"
                + "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"),
        // "Example: GET Bucket Lifecycle"
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "examplebucket.s3.amazonaws.com")
                                    .add(
                                        "x-amz-content-sha256",
                                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                                    .add("x-amz-date", "20130524T000000Z"))))
                .bucket("examplebucket")
                .forcePathStyle(false)
                .uri(URI.create("https://examplebucket.s3.amazonaws.com/?lifecycle"))
                .build(),
            SigningResult.builder()
                .putHeadersToSet(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543")
                .signature("fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543")
                .dateTime("20130524T000000Z")
                .scope("20130524/us-east-1/s3/aws4_request")
                .canonicalRequestSha(
                    "9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca")
                .build(),
            "GET\n"
                + "/\n"
                + "lifecycle=\n"
                + "host:examplebucket.s3.amazonaws.com\n"
                + "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                + "x-amz-date:20130524T000000Z\n"
                + "\n"
                + "host;x-amz-content-sha256;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        // "Example: Get Bucket (List Objects)"
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "examplebucket.s3.amazonaws.com")
                                    .add(
                                        "x-amz-content-sha256",
                                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                                    .add("x-amz-date", "20130524T000000Z"))))
                .bucket("examplebucket")
                .forcePathStyle(false)
                .uri(URI.create("https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J"))
                .build(),
            SigningResult.builder()
                .putHeadersToSet(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7")
                .signature("34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7")
                .dateTime("20130524T000000Z")
                .scope("20130524/us-east-1/s3/aws4_request")
                .canonicalRequestSha(
                    "df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7")
                .build(),
            "GET\n"
                + "/\n"
                + "max-keys=2&prefix=J\n"
                + "host:examplebucket.s3.amazonaws.com\n"
                + "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                + "x-amz-date:20130524T000000Z\n"
                + "\n"
                + "host;x-amz-content-sha256;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
  }

  static Stream<Arguments> v2() {
    // test cases taken from
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html#RESTAuthenticationExamples
    return Stream.of(
        // "Object GET" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Date", "Tue, 27 Mar 2007 19:36:42 +0000"))))
                .bucket("awsexamplebucket1")
                .forcePathStyle(false)
                .uri(
                    URI.create(
                        "https://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg"))
                .build(),
            SigningResult.builder()
                .signature("qgk2+6Sv9/oM7G3qLEjTH1a1l1g=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:qgk2+6Sv9/oM7G3qLEjTH1a1l1g=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "Object PUT" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("PUT")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Date", "Tue, 27 Mar 2007 21:15:45 +0000")
                                    .add("Content-Type", "image/jpeg")
                                    .add("Content-Length", "94328"))))
                .bucket("awsexamplebucket1")
                .forcePathStyle(false)
                .uri(
                    URI.create(
                        "https://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg"))
                .build(),
            SigningResult.builder()
                .signature("iqRzw+ileNPu1fhspnRs8nOjjIA=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:iqRzw+ileNPu1fhspnRs8nOjjIA=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "List" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("User-Agent", "Mozilla/5.0")
                                    .add("Date", "Tue, 27 Mar 2007 19:42:41 +0000"))))
                .bucket("awsexamplebucket1")
                .forcePathStyle(false)
                .uri(
                    URI.create(
                        "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/?prefix=photos&max-keys=50&marker=puppy"))
                .build(),
            SigningResult.builder()
                .signature("m0WP8eCtspQl5Ahe6L1SozdX9YA=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:m0WP8eCtspQl5Ahe6L1SozdX9YA=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "Fetch" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Date", "Tue, 27 Mar 2007 19:44:46 +0000"))))
                .bucket("awsexamplebucket1")
                .forcePathStyle(false)
                .uri(URI.create("https://awsexamplebucket1.s3.us-west-1.amazonaws.com/?acl"))
                .subresource("acl")
                .build(),
            SigningResult.builder()
                .signature("82ZHiFIjc+WbcwFKGUVEQspPn+0=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:82ZHiFIjc+WbcwFKGUVEQspPn+0=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "Delete" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("DELETE")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("User-Agent", "dotnet")
                                    .add("Date", "Tue, 27 Mar 2007 21:20:27 +0000")
                                    .add("x-amz-date", "Tue, 27 Mar 2007 21:20:26 +0000"))))
                .bucket("awsexamplebucket1")
                .forcePathStyle(false)
                .uri(
                    URI.create(
                        "https://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg"))
                .build(),
            SigningResult.builder()
                .signature("XbyTlbQdu9Xw5o8P4iMwPktxQd8=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:XbyTlbQdu9Xw5o8P4iMwPktxQd8=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "Upload" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("PUT")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("User-Agent", "curl/7.15.5")
                                    .add("Host", "static.example.com:8080")
                                    .add("Date", "Tue, 27 Mar 2007 21:06:08 +0000")
                                    .add("x-amz-acl", "public-read")
                                    .add("Content-Type", "application/x-download")
                                    .add("Content-MD5", "4gJE4saaMU4BqNR0kLY+lw==")
                                    .add("X-Amz-Meta-ReviewedBy", "joe@example.com")
                                    .add("X-Amz-Meta-ReviewedBy", "jane@example.com")
                                    .add("X-Amz-Meta-FileChecksum", "0x02661779")
                                    .add("X-Amz-Meta-ChecksumAlgorithm", "crc32")
                                    .add("Content-Disposition", "attachment; filename=database.dat")
                                    .add("Content-Encoding", "gzip")
                                    .add("Content-Length", "5913339"))))
                .bucket(null)
                .forcePathStyle(true)
                .uri(URI.create("https://static.example.com:8080/db-backup.dat.gz"))
                .build(),
            // TODO the Authorization header in the example from the AWS docs seems to be wrong
            // "AWS AKIAIOSFODNN7EXAMPLE:dKZcB+bz2EPXgSdXZp9ozGeOM4I=",
            SigningResult.builder()
                .signature("jtBQa0Aq+DkULFI8qrpwIjGEx0E=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:jtBQa0Aq+DkULFI8qrpwIjGEx0E=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "List all my buckets" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "s3.us-west-1.amazonaws.com")
                                    .add("Date", "Wed, 28 Mar 2007 01:29:59 +0000"))))
                .bucket("")
                .forcePathStyle(false)
                .uri(URI.create("https://s3.us-west-1.amazonaws.com/"))
                .build(),
            SigningResult.builder()
                .signature("qGdzdERIC03wnaRNKh6OqZehG9s=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:qGdzdERIC03wnaRNKh6OqZehG9s=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()),
        // "Unicode keys" example
        arguments(
            SigningRequest.builder()
                .httpMethodName("GET")
                .headers(
                    new HttpReqHeaders(
                        HeadersMultiMap.httpHeaders()
                            .addAll(
                                HeadersMultiMap.httpHeaders()
                                    .add("Host", "s3.us-west-1.amazonaws.com")
                                    .add("Date", "Wed, 28 Mar 2007 01:49:49 +0000"))))
                .bucket("")
                .forcePathStyle(false)
                .uri(
                    URI.create(
                        "https://s3.us-west-1.amazonaws.com/dictionary/fran%C3%A7ais/pr%c3%a9f%c3%a8re"))
                .build(),
            SigningResult.builder()
                .signature("DNEZGsoieTZ92F3bUfSPQcbGmlM=")
                .putHeadersToSet(
                    "Authorization", "AWS AKIAIOSFODNN7EXAMPLE:DNEZGsoieTZ92F3bUfSPQcbGmlM=")
                .dateTime("")
                .scope("")
                .canonicalRequestSha("")
                .build()));
  }

  @ParameterizedTest
  @MethodSource
  public void v4queryEntriesSorted(
      String rawQuery,
      String expected,
      Collection<Map.Entry<String, List<String>>> expectedEntries) {
    soft.assertThat(V4RequestSigner.queryEntriesSorted(rawQuery))
        .containsExactlyElementsOf(expectedEntries);

    soft.assertThat(
            V4RequestSigner.canonicalQuery(
                    new StringBuilder(),
                    rawQuery != null
                        ? URI.create("http://foo/?" + rawQuery)
                        : URI.create("http://foo/"))
                .toString())
        .isEqualTo(expected);
  }

  static Stream<Arguments> v4queryEntriesSorted() {
    return Stream.of(
        arguments(null, "", ImmutableList.of()),
        arguments("a", "a=", ImmutableList.of(Map.entry("a", emptyList()))),
        arguments(
            "b&a",
            "a=&b=",
            ImmutableList.builder()
                .add(Map.entry("a", emptyList()))
                .add(Map.entry("b", emptyList()))
                .build()),
        arguments(
            "b&a=foo",
            "a=foo&b=",
            ImmutableList.builder()
                .add(Map.entry("a", singletonList("foo")))
                .add(Map.entry("b", emptyList()))
                .build()),
        arguments(
            "b&a=foo&c=meh&a=bar",
            "a=foo&a=bar&b=&c=meh",
            ImmutableList.builder()
                .add(Map.entry("a", ImmutableList.of("foo", "bar")))
                .add(Map.entry("b", emptyList()))
                .add(Map.entry("c", singletonList("meh")))
                .build()));
  }
}
