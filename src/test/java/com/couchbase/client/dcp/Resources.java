/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.dcp;

import com.couchbase.client.java.json.JsonObject;
import reactor.util.annotation.Nullable;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.MissingResourceException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Helps with loading class path resources.
 * <p>
 * Can be backed by either a Class or a Classloader.
 * Note that {@link Class#getResourceAsStream(String)}
 * and {@link ClassLoader#getResourceAsStream(String)}
 * have different rules for resolving relative resource names
 * (names that do not start with a forward slash).
 * <p>
 * tldr; Use a Class if you want to refer to the
 * resource by a name relative to the Class's package.
 * <p>
 * Instances are thread-safe and reusable.
 */
public class Resources {

  private interface Loader {
    @Nullable
    InputStream getResourceAsStream(String resourceName);
  }

  private final Loader loader;
  private final String description;

  private Resources(Loader loader, String description) {
    this.loader = requireNonNull(loader);
    this.description = requireNonNull(description);
  }

  /**
   * Creates an instance that loads resources using the given class's
   * {@link Class#getResourceAsStream(String)} method.
   */
  public static Resources from(Class<?> context) {
    return new Resources(context::getResourceAsStream, context.toString());
  }

  /**
   * Creates an instance that loads resources using the given class loader's
   * {@link ClassLoader#getResourceAsStream(String)} method.
   */
  public static Resources from(ClassLoader context) {
    return new Resources(context::getResourceAsStream, context.toString());
  }

  /**
   * @throws MissingResourceException if resource is not found.
   */
  public InputStream getStream(String name) {
    InputStream is = loader.getResourceAsStream(name);
    if (is == null) {
      throw new MissingResourceException(
          "Failed to locate class path resource '" + name + "' using " + description,
          null,
          null
      );
    }
    return is;
  }

  /**
   * @throws MissingResourceException if resource is not found.
   */
  public byte[] getBytes(String name) {
    try (InputStream is = getStream(name)) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[8 * 1024];
      int len;
      while ((len = is.read(buffer)) != -1) {
        bytes.write(buffer, 0, len);
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @throws MissingResourceException if resource is not found.
   */
  public String getString(String name) {
    return new String(getBytes(name), UTF_8);
  }

  /**
   * @throws MissingResourceException if resource is not found.
   */
  public JsonObject getJsonObject(String name) {
    return JsonObject.fromJson(getBytes(name));
  }

  /**
   * @throws MissingResourceException if resource is not found.
   */
  public BufferedReader getReader(String name) {
    return new BufferedReader(new InputStreamReader(getStream(name), UTF_8));
  }

  /**
   * @param streamConverter Converts the stream of lines in the resource
   * into the value returned by this method.
   * @throws MissingResourceException if resource is not found.
   */
  public <T> T readLines(String name, Function<Stream<String>, T> streamConverter) {
    try (BufferedReader reader = getReader(name)) {
      return streamConverter.apply(reader.lines());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
