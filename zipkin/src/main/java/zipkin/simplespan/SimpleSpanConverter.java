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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.Nullable;
import zipkin.internal.Util;
import zipkin.simplespan.SimpleSpan.Kind;

import static zipkin.BinaryAnnotation.Type.BOOL;
import static zipkin.BinaryAnnotation.Type.STRING;
import static zipkin.Constants.CLIENT_ADDR;
import static zipkin.Constants.LOCAL_COMPONENT;
import static zipkin.Constants.SERVER_ADDR;

public final class SimpleSpanConverter {

  public static List<SimpleSpan> fromSpan(Span source) {
    Builders builders = new Builders(source);
    // add annotations unless they are "core"
    builders.processAnnotations(source);
    // convert binary annotations to tags and addresses
    builders.processBinaryAnnotations(source);
    return builders.build();
  }

  static class Builders {
    final List<SimpleSpan.Builder> spans = new ArrayList<>();
    Annotation cs = null, sr = null, ss = null, cr = null;

    Builders(Span source) {
      this.spans.add(newBuilder(source));
    }

    void processAnnotations(Span source) {
      for (int i = 0, length = source.annotations.size(); i < length; i++) {
        Annotation a = source.annotations.get(i);
        SimpleSpan.Builder currentSpan = forEndpoint(source, a.endpoint);
        if (currentSpan == null) continue; // drop bad data
        if (a.value.length() == 2) {
          if (a.value.equals(Constants.CLIENT_SEND)) {
            cs = a;
            currentSpan.kind(Kind.CLIENT);
          } else if (a.value.equals(Constants.SERVER_RECV)) {
            sr = a;
            currentSpan.kind(Kind.SERVER);
          } else if (a.value.equals(Constants.SERVER_SEND)) {
            ss = a;
            currentSpan.kind(Kind.SERVER);
          } else if (a.value.equals(Constants.CLIENT_RECV)) {
            cr = a;
            currentSpan.kind(Kind.CLIENT);
          } else {
            currentSpan.addAnnotation(a.timestamp, a.value);
          }
        } else {
          currentSpan.addAnnotation(a.timestamp, a.value);
        }
      }

      if (cs != null && cr != null) { // this is the client side, which is authoritative.
        maybeTimestampDuration(source, cs, cr);
      } else if (sr != null && ss != null) { // a complete server span is next authoritative
        maybeTimestampDuration(source, sr, ss);
      } else { // otherwise, the span is incomplete. revert special-casing
        revertCoreAnnotation(source, cs);
        revertCoreAnnotation(source, sr);
        revertCoreAnnotation(source, ss);
        revertCoreAnnotation(source, cr);

        if (source.timestamp != null) {
          SimpleSpan.Builder first = spans.get(0).startTimestamp(source.timestamp);
          if (source.duration != null) first.finishTimestamp(source.timestamp + source.duration);
        }
      }
    }

    void revertCoreAnnotation(Span source, Annotation a) {
      if (a == null) return;
      forEndpoint(source, a.endpoint).kind(null).addAnnotation(a.timestamp, a.value);
    }

    void maybeTimestampDuration(Span source, Annotation begin, Annotation end) {
      SimpleSpan.Builder simple = forEndpoint(source, begin.endpoint);
      if (source.timestamp != null && source.duration != null) {
        simple.startTimestamp(source.timestamp).finishTimestamp(source.timestamp + source.duration);
      } else {
        simple.startTimestamp(begin.timestamp).finishTimestamp(end.timestamp);
      }
    }

    void processBinaryAnnotations(Span source) {
      for (int i = 0, length = source.binaryAnnotations.size(); i < length; i++) {
        BinaryAnnotation b = source.binaryAnnotations.get(i);
        if (b.type == BOOL) {
          if (Constants.CLIENT_ADDR.equals(b.key)) {
            if (sr != null) forEndpoint(source, sr.endpoint).remoteEndpoint(b.endpoint);
          } else if (Constants.SERVER_ADDR.equals(b.key)) {
            if (cs != null) forEndpoint(source, cs.endpoint).remoteEndpoint(b.endpoint);
          }
          continue;
        }
        SimpleSpan.Builder currentSpan = forEndpoint(source, b.endpoint);
        if (currentSpan == null) continue; // drop bad data
        if (b.type == STRING) {
          // don't add marker "lc" tags
          if (Constants.LOCAL_COMPONENT.equals(b.key) && b.value.length == 0) continue;
          currentSpan.putTag(b.key, new String(b.value, Util.UTF_8));
        }
      }
    }

    @Nullable SimpleSpan.Builder forEndpoint(Span source, @Nullable Endpoint e) {
      if (e == null) return null;
      for (int i = 0, length = spans.size(); i < length; i++) {
        SimpleSpan.Builder next = spans.get(i);
        if (next.localEndpoint == null) {
          next.localEndpoint = e;
          return next;
        } else if (closeEnough(next.localEndpoint, e)) {
          return next;
        }
      }
      SimpleSpan.Builder result = newBuilder(source).localEndpoint(e);
      spans.add(result);
      return result;
    }

    List<SimpleSpan> build() {
      int length = spans.size();
      if (length == 1) return Collections.singletonList(spans.get(0).build());
      List<SimpleSpan> result = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        result.add(spans.get(i).build());
      }
      return result;
    }
  }

  static boolean closeEnough(Endpoint left, Endpoint right) {
    return left.equals(right);
  }

  private static SimpleSpan.Builder newBuilder(Span source) {
    return SimpleSpan.builder()
      .traceIdHigh(source.traceIdHigh)
      .traceId(source.traceId)
      .parentId(source.parentId)
      .id(source.id)
      .name(source.name)
      .debug(source.debug);
  }

  public static Span toSpan(SimpleSpan in) {
    Span.Builder result = Span.builder()
      .traceIdHigh(in.traceIdHigh())
      .traceId(in.traceId())
      .parentId(in.parentId())
      .id(in.id())
      .debug(in.debug())
      .name(in.name() == null ? "" : in.name()); // avoid a NPE

    long startTimestamp = in.startTimestamp() == null ? 0L : in.startTimestamp();
    Long finishTimestamp = in.finishTimestamp();
    if (startTimestamp != 0) {
      result.timestamp(startTimestamp);
      if (finishTimestamp != null) {
        result.duration(Math.max(finishTimestamp - startTimestamp, 1));
      }
    }

    Annotation cs = null, sr = null, ss = null, cr = null;
    String remoteEndpointType = null;

    if (in.kind() != null) {
      switch (in.kind()) {
        case CLIENT:
          remoteEndpointType = Constants.SERVER_ADDR;
          if (startTimestamp != 0L) {
            cs = Annotation.create(startTimestamp, Constants.CLIENT_SEND, in.localEndpoint());
          }
          if (finishTimestamp != null) {
            cr = Annotation.create(finishTimestamp, Constants.CLIENT_RECV, in.localEndpoint());
          }
          break;
        case SERVER:
          remoteEndpointType = Constants.CLIENT_ADDR;
          if (startTimestamp != 0L) {
            sr = Annotation.create(startTimestamp, Constants.SERVER_RECV, in.localEndpoint());
          }
          if (finishTimestamp != null) {
            ss = Annotation.create(finishTimestamp, Constants.SERVER_SEND, in.localEndpoint());
          }
          break;
        default:
          throw new AssertionError("update kind mapping");
      }
    }

    boolean hasAnnotationOrBinaryAnnotation = false;

    for (int i = 0, length = in.annotations().size(); i < length; i++) {
      Annotation a = in.annotations().get(i);
      if (in.localEndpoint() != null) {
        a = a.toBuilder().endpoint(in.localEndpoint()).build();
      }
      if (a.value.length() == 2) {
        if (a.value.equals(Constants.CLIENT_SEND)) {
          cs = a;
          remoteEndpointType = SERVER_ADDR;
        } else if (a.value.equals(Constants.SERVER_RECV)) {
          sr = a;
          remoteEndpointType = CLIENT_ADDR;
        } else if (a.value.equals(Constants.SERVER_SEND)) {
          ss = a;
        } else if (a.value.equals(Constants.CLIENT_RECV)) {
          cr = a;
        } else {
          hasAnnotationOrBinaryAnnotation = true;
          result.addAnnotation(a);
        }
      } else {
        hasAnnotationOrBinaryAnnotation = true;
        result.addAnnotation(a);
      }
    }

    for (Map.Entry<String, String> tag : in.tags().entrySet()) {
      hasAnnotationOrBinaryAnnotation = true;
      result.addBinaryAnnotation(
        BinaryAnnotation.create(tag.getKey(), tag.getValue(), in.localEndpoint()));
    }

    if (remoteEndpointType != null && in.remoteEndpoint() != null) {
      result.addBinaryAnnotation(BinaryAnnotation.address(remoteEndpointType, in.remoteEndpoint()));
    }

    if (cs != null) result.addAnnotation(cs);
    if (sr != null) result.addAnnotation(sr);
    if (ss != null) result.addAnnotation(ss);
    if (cr != null) result.addAnnotation(cr);

    // don't report server-side timestamp on shared or incomplete spans
    if (Boolean.TRUE.equals(in.shared()) && sr != null) {
      result.timestamp(null).duration(null);
    }
    // don't report client span.timestamp if unfinished.
    // This allows one-way to be modeled as span.kind(serverOrClient).start().flush()
    if (cs != null && finishTimestamp == null) {
      result.timestamp(null);
    }
    if (!hasAnnotationOrBinaryAnnotation) { // create a small dummy annotation
      result.addBinaryAnnotation(BinaryAnnotation.create(LOCAL_COMPONENT, "", in.localEndpoint()));
    }
    return result.build();
  }
}
