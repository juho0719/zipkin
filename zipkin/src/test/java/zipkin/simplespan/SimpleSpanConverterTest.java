/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.simplespan;

import org.junit.Test;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.TraceKeys;
import zipkin.internal.Util;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin.Constants.LOCAL_COMPONENT;

public class SimpleSpanConverterTest {
  Endpoint frontend = Endpoint.create("frontend", 127 << 24 | 1);
  Endpoint backend = Endpoint.builder()
    .serviceName("backend")
    .ipv4(192 << 24 | 168 << 16 | 99 << 8 | 101)
    .port(9000)
    .build();

  @Test public void client() {

    SimpleSpan simpleClient = SimpleSpan.builder()
      .traceId("7180c278b62e8f6a216a2aea45d08fc9")
      .parentId("6b221d5bc9e6496c")
      .id("5b4185666d50f68b")
      .name("get")
      .kind(SimpleSpan.Kind.CLIENT)
      .localEndpoint(frontend)
      .remoteEndpoint(backend)
      .startTimestamp(1472470996199000L)
      .finishTimestamp(1472470996199000L + 207000L)
      .addAnnotation(1472470996238000L, Constants.WIRE_SEND)
      .addAnnotation(1472470996403000L, Constants.WIRE_RECV)
      .putTag(TraceKeys.HTTP_PATH, "/api")
      .putTag("clnt/finagle.version", "6.45.0")
      .build();

    Span client = Span.builder()
      .traceIdHigh(Util.lowerHexToUnsignedLong("7180c278b62e8f6a"))
      .traceId(Util.lowerHexToUnsignedLong("216a2aea45d08fc9"))
      .parentId(Util.lowerHexToUnsignedLong("6b221d5bc9e6496c"))
      .id(Util.lowerHexToUnsignedLong("5b4185666d50f68b"))
      .name("get")
      .timestamp(1472470996199000L)
      .duration(207000L)
      .addAnnotation(Annotation.create(1472470996199000L, Constants.CLIENT_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996238000L, Constants.WIRE_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996403000L, Constants.WIRE_RECV, frontend))
      .addAnnotation(Annotation.create(1472470996406000L, Constants.CLIENT_RECV, frontend))
      .addBinaryAnnotation(BinaryAnnotation.create(TraceKeys.HTTP_PATH, "/api", frontend))
      .addBinaryAnnotation(BinaryAnnotation.create("clnt/finagle.version", "6.45.0", frontend))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.SERVER_ADDR, backend))
      .build();

    assertThat(SimpleSpanConverter.toSpan(simpleClient))
      .isEqualTo(client);
    assertThat(SimpleSpanConverter.fromSpan(client))
      .containsExactly(simpleClient);
  }

  @Test public void server() {
    SimpleSpan simpleServer = SimpleSpan.builder()
      .traceId("7180c278b62e8f6a216a2aea45d08fc9")
      .id("216a2aea45d08fc9")
      .name("get")
      .kind(SimpleSpan.Kind.SERVER)
      .localEndpoint(backend)
      .remoteEndpoint(frontend)
      .startTimestamp(1472470996199000L)
      .finishTimestamp(1472470996199000L + 207000L)
      .putTag(TraceKeys.HTTP_PATH, "/api")
      .putTag("clnt/finagle.version", "6.45.0")
      .build();

    Span server = Span.builder()
      .traceIdHigh(Util.lowerHexToUnsignedLong("7180c278b62e8f6a"))
      .traceId(Util.lowerHexToUnsignedLong("216a2aea45d08fc9"))
      .id(Util.lowerHexToUnsignedLong("216a2aea45d08fc9"))
      .name("get")
      .timestamp(1472470996199000L)
      .duration(207000L)
      .addAnnotation(Annotation.create(1472470996199000L, Constants.SERVER_RECV, backend))
      .addAnnotation(Annotation.create(1472470996406000L, Constants.SERVER_SEND, backend))
      .addBinaryAnnotation(BinaryAnnotation.create(TraceKeys.HTTP_PATH, "/api", backend))
      .addBinaryAnnotation(BinaryAnnotation.create("clnt/finagle.version", "6.45.0", backend))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.CLIENT_ADDR, frontend))
      .build();

    assertThat(SimpleSpanConverter.toSpan(simpleServer))
      .isEqualTo(server);
    assertThat(SimpleSpanConverter.fromSpan(server))
      .containsExactly(simpleServer);
  }

  @Test public void localSpan_emptyComponent() {
    SimpleSpan simpleLocal = SimpleSpan.builder()
      .traceId(1L)
      .parentId(1L)
      .id(2L)
      .name("local")
      .localEndpoint(frontend)
      .startTimestamp(1472470996199000L)
      .finishTimestamp(1472470996199000L + 207000L)
      .build();

    Span local = Span.builder()
      .traceId(1L)
      .parentId(1L)
      .id(2L)
      .name("local")
      .timestamp(1472470996199000L).duration(207000L)
      .addBinaryAnnotation(BinaryAnnotation.create(LOCAL_COMPONENT, "", frontend)).build();

    assertThat(SimpleSpanConverter.toSpan(simpleLocal))
      .isEqualTo(local);
    assertThat(SimpleSpanConverter.fromSpan(local))
      .containsExactly(simpleLocal);
  }

  @Test public void clientAndServer() {
    Span shared = Span.builder()
      .traceIdHigh(Util.lowerHexToUnsignedLong("7180c278b62e8f6a"))
      .traceId(Util.lowerHexToUnsignedLong("216a2aea45d08fc9"))
      .parentId(Util.lowerHexToUnsignedLong("6b221d5bc9e6496c"))
      .id(Util.lowerHexToUnsignedLong("5b4185666d50f68b"))
      .name("get")
      .timestamp(1472470996199000L)
      .duration(207000L)
      .addAnnotation(Annotation.create(1472470996199000L, Constants.CLIENT_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996238000L, Constants.WIRE_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996250000L, Constants.SERVER_RECV, backend))
      .addAnnotation(Annotation.create(1472470996350000L, Constants.SERVER_SEND, backend))
      .addAnnotation(Annotation.create(1472470996403000L, Constants.WIRE_RECV, frontend))
      .addAnnotation(Annotation.create(1472470996406000L, Constants.CLIENT_RECV, frontend))
      .addBinaryAnnotation(BinaryAnnotation.create(TraceKeys.HTTP_PATH, "/api", frontend))
      .addBinaryAnnotation(
        BinaryAnnotation.create(TraceKeys.HTTP_URL, "http://backend/api", backend))
      .addBinaryAnnotation(BinaryAnnotation.create("clnt/finagle.version", "6.45.0", frontend))
      .addBinaryAnnotation(BinaryAnnotation.create("srv/finagle.version", "6.44.0", backend))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.CLIENT_ADDR, frontend))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.SERVER_ADDR, backend))
      .build();

    SimpleSpan.Builder builder = SimpleSpan.builder()
      .traceId("7180c278b62e8f6a216a2aea45d08fc9")
      .parentId("6b221d5bc9e6496c")
      .id("5b4185666d50f68b")
      .name("get");

    // the client side owns timestamp and duration
    SimpleSpan client = builder.clone().kind(SimpleSpan.Kind.CLIENT)
      .localEndpoint(frontend)
      .remoteEndpoint(backend)
      .startTimestamp(1472470996199000L)
      .finishTimestamp(1472470996199000L + 207000L)
      .addAnnotation(1472470996238000L, Constants.WIRE_SEND)
      .addAnnotation(1472470996403000L, Constants.WIRE_RECV)
      .putTag(TraceKeys.HTTP_PATH, "/api")
      .putTag("clnt/finagle.version", "6.45.0")
      .build();

    // notice server tags are different than the client, and the client's annotations aren't here
    SimpleSpan server = builder.clone().kind(SimpleSpan.Kind.SERVER)
      .localEndpoint(backend)
      .remoteEndpoint(frontend)
      .putTag(TraceKeys.HTTP_URL, "http://backend/api")
      .putTag("srv/finagle.version", "6.44.0")
      .build();

    assertThat(SimpleSpanConverter.fromSpan(shared))
      .containsExactly(client, server);
  }
}
