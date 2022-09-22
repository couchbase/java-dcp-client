/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 * Provides global access to the services of a Jackson {@link ObjectMapper}
 * with default configuration.
 */
public class DefaultObjectMapper {
  private DefaultObjectMapper() {
    throw new AssertionError("not instantiable");
  }

  // In order to prevent inappropriate reconfiguration, the mapper itself is not exposed.
  // Instead, immutable readers and writers are made available for advanced use cases.
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectReader reader = mapper.reader();
  private static final ObjectWriter writer = mapper.writer();
  private static final ObjectWriter prettyWriter = writer.withDefaultPrettyPrinter();

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
  };

  public static ObjectReader reader() {
    return reader;
  }

  public static ObjectWriter writer() {
    return writer;
  }

  public static ObjectWriter prettyWriter() {
    return prettyWriter;
  }

  public static String writeValueAsString(Object value) throws JsonProcessingException {
    return mapper.writeValueAsString(value);
  }

  public static byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
    return mapper.writeValueAsBytes(value);
  }

  public static void writeValue(OutputStream out, Object value) throws IOException {
    mapper.writeValue(out, value);
  }

  public static void writeValue(Writer w, Object value) throws IOException {
    mapper.writeValue(w, value);
  }

  public static <T> T readValue(String content, Class<T> valueType) throws IOException {
    return mapper.readValue(content, valueType);
  }

  public static <T> T readValue(String content, TypeReference<T> valueTypeRef) throws IOException {
    return mapper.readValue(content, valueTypeRef);
  }

  public static <T> T readValue(byte[] content, Class<T> valueType) throws IOException {
    return mapper.readValue(content, valueType);
  }

  public static <T> T readValue(byte[] content, int offset, int len, Class<T> valueType) throws IOException {
    return mapper.readValue(content, offset, len, valueType);
  }

  public static <T> T readValue(byte[] content, TypeReference<T> valueTypeRef) throws IOException {
    return mapper.readValue(content, valueTypeRef);
  }

  public static <T> T readValue(byte[] content, int offset, int len, TypeReference<T> valueTypeRef) throws IOException {
    return mapper.readValue(content, offset, len, valueTypeRef);
  }

  public static <T> T readValue(Reader src, Class<T> valueType) throws IOException {
    return mapper.readValue(src, valueType);
  }

  public static <T> T readValue(Reader src, TypeReference<T> valueTypeRef) throws IOException {
    return mapper.readValue(src, valueTypeRef);
  }

  public static <T> T readValue(InputStream src, Class<T> valueType) throws IOException {
    return mapper.readValue(src, valueType);
  }

  public static <T> T readValue(InputStream src, TypeReference<T> valueTypeRef) throws IOException {
    return mapper.readValue(src, valueTypeRef);
  }

  public static Map<String, Object> readValueAsMap(String content) throws IOException {
    return mapper.readValue(content, MAP_TYPE);
  }

  public static Map<String, Object> readValueAsMap(byte[] content) throws IOException {
    return readValue(content, MAP_TYPE);
  }

  public static Map<String, Object> readValueAsMap(Reader src) throws IOException {
    return mapper.readValue(src, MAP_TYPE);
  }

  public static Map<String, Object> readValueAsMap(InputStream src) throws IOException {
    return mapper.readValue(src, MAP_TYPE);
  }

  public static JsonNode readTree(String content) throws IOException {
    return mapper.readTree(content);
  }

  public static JsonNode readTree(byte[] content) throws IOException {
    return mapper.readTree(content);
  }

  public static JsonNode readTree(Reader src) throws IOException {
    return mapper.readTree(src);
  }

  public static JsonNode readTree(InputStream src) throws IOException {
    return mapper.readTree(src);
  }
}
