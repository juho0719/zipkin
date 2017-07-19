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
package zipkin.internal;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import zipkin.internal.JsonCodec.JsonReaderAdapter;
import zipkin.simplespan.SimpleSpan;
import zipkin.simplespan.SimpleSpanCodec;

import static zipkin.internal.Buffer.asciiSizeInBytes;
import static zipkin.internal.Buffer.jsonEscapedSizeInBytes;
import static zipkin.internal.JsonCodec.ANNOTATION_WRITER;
import static zipkin.internal.JsonCodec.ENDPOINT_READER;
import static zipkin.internal.JsonCodec.ENDPOINT_WRITER;
import static zipkin.internal.JsonCodec.writeList;

/**
 * Internal type supporting codec operations in {@link SimpleSpan}. Design rationale is the same as
 * {@link JsonCodec}.
 */
public final class SimpleSpanJsonCodec implements SimpleSpanCodec {

  @Override public SimpleSpan readSpan(byte[] bytes) {
    return JsonCodec.read(new SimpleSpanReader(), bytes);
  }

  /** Serialize a span recorded from instrumentation into its binary form. */
  @Override public byte[] writeSpan(SimpleSpan span) {
    return JsonCodec.write(SPAN_WRITER, span);
  }

  @Override public List<SimpleSpan> readSpans(byte[] bytes) {
    return JsonCodec.readList(new SimpleSpanReader(), bytes);
  }

  @Override public byte[] writeSpans(List<SimpleSpan> value) {
    return writeList(SPAN_WRITER, value);
  }

  static final class SimpleSpanReader implements JsonReaderAdapter<SimpleSpan> {
    SimpleSpan.Builder builder;

    @Override public SimpleSpan fromJson(JsonReader reader) throws IOException {
      if (builder == null) {
        builder = SimpleSpan.builder();
      } else {
        builder.clear();
      }
      reader.beginObject();
      while (reader.hasNext()) {
        String nextName = reader.nextName();
        if (nextName.equals("traceId")) {
          builder.traceId(reader.nextString());
        } else if (nextName.equals("parentId") && reader.peek() != JsonToken.NULL) {
          builder.parentId(reader.nextString());
        } else if (nextName.equals("id")) {
          builder.id(reader.nextString());
        } else if (nextName.equals("kind")) {
          builder.kind(SimpleSpan.Kind.valueOf(reader.nextString()));
        } else if (nextName.equals("name") && reader.peek() != JsonToken.NULL) {
          builder.name(reader.nextString());
        } else if (nextName.equals("startTimestamp") && reader.peek() != JsonToken.NULL) {
          builder.startTimestamp(reader.nextLong());
        } else if (nextName.equals("finishTimestamp") && reader.peek() != JsonToken.NULL) {
          builder.finishTimestamp(reader.nextLong());
        } else if (nextName.equals("localEndpoint")) {
          builder.localEndpoint(ENDPOINT_READER.fromJson(reader));
        } else if (nextName.equals("remoteEndpoint") && reader.peek() != JsonToken.NULL) {
          builder.remoteEndpoint(ENDPOINT_READER.fromJson(reader));
        } else if (nextName.equals("annotations")) {
          reader.beginArray();
          while (reader.hasNext()) {
            reader.beginObject();
            Long timestamp = null;
            String value = null;
            while (reader.hasNext()) {
              nextName = reader.nextName();
              if (nextName.equals("timestamp")) {
                timestamp = reader.nextLong();
              } else if (nextName.equals("value")) {
                value = reader.nextString();
              } else {
                reader.skipValue();
              }
            }
            reader.endObject();
            if (timestamp != null && value != null) builder.addAnnotation(timestamp, value);
          }
          reader.endArray();
        } else if (nextName.equals("tags")) {
          reader.beginObject();
          while (reader.hasNext()) {
            String key = reader.nextName();
            if (reader.peek() == JsonToken.NULL) {
              throw new MalformedJsonException("No value at " + reader.getPath());
            }
            builder.putTag(key, reader.nextString());
          }
          reader.endObject();
        } else if (nextName.equals("debug") && reader.peek() != JsonToken.NULL) {
          if (reader.nextBoolean()) builder.debug(true);
        } else if (nextName.equals("shared") && reader.peek() != JsonToken.NULL) {
          if (reader.nextBoolean()) builder.shared(true);
        } else {
          reader.skipValue();
        }
      }
      reader.endObject();
      return builder.build();
    }

    @Override public String toString() {
      return "SimpleSpan";
    }
  }

  static final Buffer.Writer<SimpleSpan> SPAN_WRITER = new Buffer.Writer<SimpleSpan>() {
    @Override public int sizeInBytes(SimpleSpan value) {
      int sizeInBytes = 0;
      if (value.traceIdHigh() != 0) sizeInBytes += 16;
      sizeInBytes += asciiSizeInBytes("{\"traceId\":\"") + 16 + 1;
      if (value.parentId() != null) {
        sizeInBytes += asciiSizeInBytes(",\"parentId\":\"") + 16 + 1;
      }
      sizeInBytes += asciiSizeInBytes(",\"id\":\"") + 16 + 1;
      if (value.kind() != null) {
        sizeInBytes += asciiSizeInBytes(",\"kind\":\"");
        sizeInBytes += asciiSizeInBytes(value.kind().toString()) + 1;
      }
      if (value.name() != null) {
        sizeInBytes += asciiSizeInBytes(",\"name\":\"");
        sizeInBytes += jsonEscapedSizeInBytes(value.name()) + 1;
      }
      if (value.startTimestamp() != null) {
        sizeInBytes += asciiSizeInBytes(",\"startTimestamp\":");
        sizeInBytes += asciiSizeInBytes(value.startTimestamp());
      }
      if (value.finishTimestamp() != null) {
        sizeInBytes += asciiSizeInBytes(",\"finishTimestamp\":");
        sizeInBytes += asciiSizeInBytes(value.finishTimestamp());
      }
      sizeInBytes += asciiSizeInBytes(",\"localEndpoint\":");
      sizeInBytes += ENDPOINT_WRITER.sizeInBytes(value.localEndpoint());
      if (value.remoteEndpoint() != null) {
        sizeInBytes += asciiSizeInBytes(",\"remoteEndpoint\":");
        sizeInBytes += ENDPOINT_WRITER.sizeInBytes(value.remoteEndpoint());
      }
      if (!value.annotations().isEmpty()) {
        sizeInBytes += asciiSizeInBytes(",\"annotations\":");
        sizeInBytes += JsonCodec.sizeInBytes(ANNOTATION_WRITER, value.annotations());
      }
      if (!value.tags().isEmpty()) {
        sizeInBytes += asciiSizeInBytes(",\"tags\":");
        sizeInBytes += 2; // curly braces
        int tagCount = value.tags().size();
        if (tagCount > 1) sizeInBytes += tagCount - 1; // comma to join elements
        for (Map.Entry<String, String> entry : value.tags().entrySet()) {
          sizeInBytes += 5; // 4 quotes and a colon
          sizeInBytes += Buffer.jsonEscapedSizeInBytes(entry.getKey());
          sizeInBytes += Buffer.jsonEscapedSizeInBytes(entry.getValue());
        }
      }
      if (Boolean.TRUE.equals(value.debug())) {
        sizeInBytes += asciiSizeInBytes(",\"debug\":true");
      }
      if (Boolean.TRUE.equals(value.shared())) {
        sizeInBytes += asciiSizeInBytes(",\"shared\":true");
      }
      return ++sizeInBytes;// end curly-brace
    }

    @Override public void write(SimpleSpan value, Buffer b) {
      b.writeAscii("{\"traceId\":\"");
      if (value.traceIdHigh() != 0) {
        b.writeLowerHex(value.traceIdHigh());
      }
      b.writeLowerHex(value.traceId()).writeByte('"');
      if (value.parentId() != null) {
        b.writeAscii(",\"parentId\":\"").writeLowerHex(value.parentId()).writeByte('"');
      }
      b.writeAscii(",\"id\":\"").writeLowerHex(value.id()).writeByte('"');
      if (value.kind() != null) {
        b.writeAscii(",\"kind\":\"").writeJsonEscaped(value.kind().toString()).writeByte('"');
        ;
      }
      if (value.name() != null) {
        b.writeAscii(",\"name\":\"").writeJsonEscaped(value.name()).writeByte('"');
      }
      if (value.startTimestamp() != null) {
        b.writeAscii(",\"startTimestamp\":").writeAscii(value.startTimestamp());
      }
      if (value.finishTimestamp() != null) {
        b.writeAscii(",\"finishTimestamp\":").writeAscii(value.finishTimestamp());
      }
      if (value.localEndpoint() != null) {
        b.writeAscii(",\"localEndpoint\":");
        ENDPOINT_WRITER.write(value.localEndpoint(), b);
      }
      if (value.remoteEndpoint() != null) {
        b.writeAscii(",\"remoteEndpoint\":");
        ENDPOINT_WRITER.write(value.remoteEndpoint(), b);
      }
      if (!value.annotations().isEmpty()) {
        b.writeAscii(",\"annotations\":");
        writeList(ANNOTATION_WRITER, value.annotations(), b);
      }
      if (!value.tags().isEmpty()) {
        b.writeAscii(",\"tags\":{");
        Iterator<Map.Entry<String, String>> i = value.tags().entrySet().iterator();
        while (i.hasNext()) {
          Map.Entry<String, String> entry = i.next();
          b.writeByte('"').writeJsonEscaped(entry.getKey()).writeAscii("\":\"");
          b.writeJsonEscaped(entry.getValue()).writeByte('"');
          if (i.hasNext()) b.writeByte(',');
        }
        b.writeByte('}');
      }
      if (Boolean.TRUE.equals(value.debug())) {
        b.writeAscii(",\"debug\":true");
      }
      if (Boolean.TRUE.equals(value.debug())) {
        b.writeAscii(",\"shared\":true");
      }
      b.writeByte('}');
    }

    @Override public String toString() {
      return "SimpleSpan";
    }
  };
}
