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
package org.projectnessie.s3mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.s3mock.IcebergS3Mock.S3MockServer;
import org.projectnessie.s3mock.data.Bucket;
import org.projectnessie.s3mock.data.Buckets;
import org.projectnessie.s3mock.data.ErrorResponse;
import org.projectnessie.s3mock.data.ListAllMyBucketsResult;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergS3MockServer extends AbstractIcebergS3MockServer {

  public static final String BUCKET = "bucket";
  public static final String MY_OBJECT_KEY = "my/object/key";
  public static final String MY_OBJECT_NAME = "my-object-name";

  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void smoke() throws Exception {
    @SuppressWarnings("resource")
    JsonNode node = doGet(createServer(b -> {}).getBaseUri().resolve("ready"), JsonNode.class);
    soft.assertThat(node.get("ready")).isEqualTo(BooleanNode.TRUE);
  }

  @Test
  public void listBuckets() throws Exception {
    @SuppressWarnings("resource")
    S3MockServer server =
        createServer(
            b ->
                b.putBuckets("secret", S3Bucket.builder().build())
                    .putBuckets(BUCKET, S3Bucket.builder().build()));

    ListAllMyBucketsResult result = doGet(server.getBaseUri(), ListAllMyBucketsResult.class);
    soft.assertThat(result)
        .extracting(ListAllMyBucketsResult::buckets)
        .extracting(Buckets::buckets)
        .asList()
        .map(Bucket.class::cast)
        .map(Bucket::name)
        .containsExactlyInAnyOrder(BUCKET, "secret");

    soft.assertThat(doHead(server.getBaseUri().resolve(BUCKET))).isEqualTo(200);
    soft.assertThat(doHead(server.getBaseUri().resolve("secret"))).isEqualTo(200);
    soft.assertThat(doHead(server.getBaseUri().resolve("foo"))).isEqualTo(404);
    soft.assertThat(doHead(server.getBaseUri().resolve("bar"))).isEqualTo(404);
  }

  @Test
  public void headObject() throws Exception {
    @SuppressWarnings("resource")
    S3MockServer server = createServer(b -> b.putBuckets(BUCKET, S3Bucket.builder().build()));

    soft.assertThat(doHead(server.getBaseUri().resolve(BUCKET + "/" + MY_OBJECT_NAME)))
        .isEqualTo(404);
    soft.assertThat(doHead(server.getBaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY)))
        .isEqualTo(404);
  }

  @Test
  public void getObject() throws Exception {
    Map<String, MockObject> objects = new HashMap<>();

    @SuppressWarnings("resource")
    S3MockServer server =
        createServer(
            b ->
                b.putBuckets(
                    BUCKET, S3Bucket.builder().object(objects::get).deleter(o -> false).build()));

    String content = "Hello World\nHello Nessie!";

    MockObject obj =
        ImmutableMockObject.builder()
            .contentLength(content.length())
            .contentType("text/plain")
            .writer((range, w) -> w.write(content.getBytes(StandardCharsets.UTF_8)))
            .build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(doGet(server.getBaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY), String.class))
        .isEqualTo(content);
    soft.assertThat(
            doGet(server.getBaseUri().resolve(BUCKET + "/" + MY_OBJECT_NAME), ErrorResponse.class))
        .extracting(ErrorResponse::code)
        .isEqualTo("NoSuchKey");
    soft.assertThat(
            doGetStatus(
                server.getBaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY),
                Collections.singletonMap("If-None-Match", obj.etag())))
        .isEqualTo(304);
  }

  public <T> T doGet(URI uri, Class<T> type) throws Exception {
    return doGet(uri, type, Collections.emptyMap());
  }

  public int doGetStatus(URI uri, Map<String, String> headers) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    headers.forEach(conn::addRequestProperty);
    conn.connect();
    return ((HttpURLConnection) conn).getResponseCode();
  }

  @SuppressWarnings("unchecked")
  public <T> T doGet(URI uri, Class<T> type, Map<String, String> headers) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    headers.forEach(conn::addRequestProperty);
    conn.connect();
    String contentType = conn.getHeaderField("Content-Type");

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    InputStream in = null;
    try {
      in = ((HttpURLConnection) conn).getErrorStream();
      if (in == null) {
        in = conn.getInputStream();
      }
      ByteStreams.copy(in, out);
    } finally {
      if (in != null) {
        in.close();
      }
    }

    if (contentType != null && contentType.endsWith("xml")) {
      return XML_MAPPER.readValue(out.toByteArray(), type);
    }
    if (contentType != null && contentType.endsWith("json")) {
      return JSON_MAPPER.readValue(out.toByteArray(), type);
    }
    if (type.isAssignableFrom(String.class)) {
      return (T) out.toString("UTF-8");
    }
    return (T) out.toByteArray();
  }

  public int doHead(URI uri) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    ((HttpURLConnection) conn).setRequestMethod("HEAD");
    conn.connect();
    return ((HttpURLConnection) conn).getResponseCode();
  }

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper XML_MAPPER = new XmlMapper();
}
