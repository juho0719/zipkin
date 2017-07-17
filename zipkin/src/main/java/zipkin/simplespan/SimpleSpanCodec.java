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

import java.util.List;
import zipkin.internal.SimpleSpanJsonCodec;

/** Utilities for working with {@link SimpleSpan} */
public interface SimpleSpanCodec {
  SimpleSpanCodec JSON = new SimpleSpanJsonCodec();

  /** Serialize a span recorded from instrumentation into its binary form. */
  byte[] writeSpan(SimpleSpan span);

  /** Serialize a list of spans recorded from instrumentation into their binary form. */
  byte[] writeSpans(List<SimpleSpan> spans);

  /** throws {@linkplain IllegalArgumentException} if a span couldn't be decoded */
  SimpleSpan readSpan(byte[] bytes);

  /** throws {@linkplain IllegalArgumentException} if the spans couldn't be decoded */
  List<SimpleSpan> readSpans(byte[] bytes);
}
