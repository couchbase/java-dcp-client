/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class JacksonHelper {
  private static final JsonMapper mapper = new JsonMapper();

  public static ObjectNode readObject(byte[] json) {
    try {
      return (ObjectNode) mapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Expected a JSON object but got something else", e);
    }
  }

  public static ObjectNode readObject(String json) {
    try {
      return (ObjectNode) mapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Expected a JSON object but got something else", e);
    }
  }

  public static <T> T convertValue(Object from, TypeReference<T> toValueTypeRef) {
    return mapper.convertValue(from, toValueTypeRef);
  }

  public static ObjectReader reader() {
    return mapper.reader();
  }

  /**
   * Returns a list where each element is the result of applying the given transform
   * to the corresponding element of the ArrayNode.
   */
  public static <T> List<T> transform(ArrayNode array, Function<JsonNode, ? extends T> transformer) {
    List<T> result = new ArrayList<>();
    array.forEach(elementNode -> result.add(transformer.apply(elementNode)));
    return result;
  }
}
