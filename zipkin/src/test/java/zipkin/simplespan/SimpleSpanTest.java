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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import okio.Buffer;
import okio.ByteString;
import org.junit.Test;
import zipkin.Annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static zipkin.TestObjects.APP_ENDPOINT;

public class SimpleSpanTest {
  SimpleSpan base = SimpleSpan.builder().traceId(1L).id(1L).localEndpoint(APP_ENDPOINT).build();

  @Test public void spanNamesLowercase() {
    assertThat(base.toBuilder().name("GET").build().name())
      .isEqualTo("get");
  }

  @Test public void annotationsSortByTimestamp() {
    SimpleSpan span = base.toBuilder()
      .addAnnotation(2L, "foo")
      .addAnnotation(1L, "foo")
      .build();

    // note: annotations don't also have endpoints, as it is implicit to SimpleSpan.localEndpoint
    assertThat(span.annotations()).containsExactly(
      Annotation.create(1L, "foo", null),
      Annotation.create(2L, "foo", null)
    );
  }

  @Test public void putTagOverwritesValue() {
    SimpleSpan span = base.toBuilder()
      .putTag("foo", "bar")
      .putTag("foo", "qux")
      .build();

    assertThat(span.tags()).containsExactly(
      entry("foo", "qux")
    );
  }

  @Test public void clone_differentCollections() {
    SimpleSpan.Builder builder = base.toBuilder()
      .addAnnotation(1L, "foo")
      .putTag("foo", "qux");

    SimpleSpan.Builder builder2 = builder.clone()
      .addAnnotation(2L, "foo")
      .putTag("foo", "bar");

    assertThat(builder.build()).isEqualTo(base.toBuilder()
      .addAnnotation(1L, "foo")
      .putTag("foo", "qux")
      .build()
    );

    assertThat(builder2.build()).isEqualTo(base.toBuilder()
      .addAnnotation(1L, "foo")
      .addAnnotation(2L, "foo")
      .putTag("foo", "bar")
      .build()
    );
  }

  @Test public void toString_isJson() {
    assertThat(base.toString()).hasToString(
      "{\"traceId\":\"0000000000000001\",\"id\":\"0000000000000001\",\"localEndpoint\":{\"serviceName\":\"app\",\"ipv4\":\"172.17.0.2\",\"port\":8080}}"
    );
  }

  /** Catches common error when zero is passed instead of null for a timestamp */
  @Test public void coercesZeroTimestampsToNull() {
    SimpleSpan span = base.toBuilder()
      .startTimestamp(0L)
      .finishTimestamp(0L)
      .build();

    assertThat(span.startTimestamp())
      .isNull();
    assertThat(span.finishTimestamp())
      .isNull();
  }

  @Test public void serialization() throws Exception {
    Buffer buffer = new Buffer();
    new ObjectOutputStream(buffer.outputStream()).writeObject(base);

    assertThat(new ObjectInputStream(buffer.inputStream()).readObject())
      .isEqualTo(base);
  }

  @Test public void serializationUsesJson() throws Exception {
    Buffer buffer = new Buffer();
    new ObjectOutputStream(buffer.outputStream()).writeObject(base);

    assertThat(buffer.indexOf(ByteString.encodeUtf8(base.toString())))
      .isPositive();
  }
}
