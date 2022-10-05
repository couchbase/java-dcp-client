/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.core.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;


public class CbCollections {
  private CbCollections() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns a new unmodifiable list with the same contents as the given collection.
   *
   * @param c may be {@code null}, in which case an empty list is returned.
   */
  public static <T> List<T> copyToUnmodifiableList(Collection<T> c) {
    return isNullOrEmpty(c) ? emptyList() : unmodifiableList(new ArrayList<>(c));
  }

  /**
   * Returns a new unmodifiable set with the same contents as the given collection.
   *
   * @param c may be {@code null}, in which case an empty set is returned.
   */
  public static <T> Set<T> copyToUnmodifiableSet(Collection<T> c) {
    return isNullOrEmpty(c) ? emptySet() : unmodifiableSet(new HashSet<>(c));
  }

  public static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.isEmpty();
  }

  public static boolean isNullOrEmpty(Map<?, ?> m) {
    return m == null || m.isEmpty();
  }

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  /**
   * Returns an unmodifiable set containing the given items.
   *
   * @throws NullPointerException     if any item is null
   * @throws IllegalArgumentException if there are duplicate items
   */
  @SafeVarargs
  public static <T> Set<T> setOf(T... items) {
    Set<T> result = new HashSet<>();
    for (T item : items) {
      if (!result.add(requireNonNull(item, "Set may not contain null"))) {
        throw new IllegalArgumentException("Duplicate item: " + item);
      }
    }
    return unmodifiableSet(result);
  }

  /**
   * Returns an unmodifiable list containing the given items.
   *
   * @throws NullPointerException if any item is null
   */
  @SafeVarargs
  public static <T> List<T> listOf(T... items) {
    List<T> result = new ArrayList<>(items.length);
    for (T item : items) {
      result.add(requireNonNull(item, "List may not contain null"));
    }
    return unmodifiableList(result);
  }

  /**
   * Returns an unmodifiable empty map.
   */
  public static <K, V> Map<K, V> mapOf() {
    return emptyMap();
  }

  /**
   * Returns an unmodifiable map containing the given key/value pairs.
   *
   * @throws NullPointerException     if any key or value is null
   * @throws IllegalArgumentException if there are duplicate keys
   */
  public static <K, V> Map<K, V> mapOf(K key1, V value1) {
    Map<K, V> result = new HashMap<>();
    putUniqueKey(result, key1, value1);
    return unmodifiableMap(result);
  }

  /**
   * Returns an unmodifiable map containing the given key/value pairs.
   *
   * @throws NullPointerException     if any key or value is null
   * @throws IllegalArgumentException if there are duplicate keys
   */
  public static <K, V> Map<K, V> mapOf(K key1, V value1,
                                       K key2, V value2) {
    Map<K, V> result = new HashMap<>();
    putUniqueKey(result, key1, value1);
    putUniqueKey(result, key2, value2);
    return unmodifiableMap(result);
  }

  /**
   * Returns an unmodifiable map containing the given key/value pairs.
   *
   * @throws NullPointerException     if any key or value is null
   * @throws IllegalArgumentException if there are duplicate keys
   */
  public static <K, V> Map<K, V> mapOf(K key1, V value1,
                                       K key2, V value2,
                                       K key3, V value3) {
    Map<K, V> result = new HashMap<>();
    putUniqueKey(result, key1, value1);
    putUniqueKey(result, key2, value2);
    putUniqueKey(result, key3, value3);
    return unmodifiableMap(result);
  }

  /**
   * Returns an unmodifiable map containing the given key/value pairs.
   *
   * @throws NullPointerException     if any key or value is null
   * @throws IllegalArgumentException if there are duplicate keys
   */
  @SuppressWarnings("Duplicates")
  public static <K, V> Map<K, V> mapOf(K key1, V value1,
                                       K key2, V value2,
                                       K key3, V value3,
                                       K key4, V value4) {
    Map<K, V> result = new HashMap<>();
    putUniqueKey(result, key1, value1);
    putUniqueKey(result, key2, value2);
    putUniqueKey(result, key3, value3);
    putUniqueKey(result, key4, value4);
    return unmodifiableMap(result);
  }

  /**
   * Returns an unmodifiable map containing the given key/value pairs.
   *
   * @throws NullPointerException     if any key or value is null
   * @throws IllegalArgumentException if there are duplicate keys
   */
  @SuppressWarnings("Duplicates")
  public static <K, V> Map<K, V> mapOf(K key1, V value1,
                                       K key2, V value2,
                                       K key3, V value3,
                                       K key4, V value4,
                                       K key5, V value5) {
    Map<K, V> result = new HashMap<>();
    putUniqueKey(result, key1, value1);
    putUniqueKey(result, key2, value2);
    putUniqueKey(result, key3, value3);
    putUniqueKey(result, key4, value4);
    putUniqueKey(result, key5, value5);
    return unmodifiableMap(result);
  }

  private static <K, V> void putUniqueKey(Map<K, V> map, K key, V value) {
    requireNonNull(key, "Key may not be null.");
    requireNonNull(value, "Value may not be null.");

    if (map.put(key, value) != null) {
      throw new IllegalArgumentException("Duplicate key: " + key);
    }
  }

  public static <T1, T2> List<T2> transform(Collection<? extends T1> original, Function<? super T1, ? extends T2> transformer) {
    return original.stream()
        .map(transformer)
        .collect(Collectors.toList());
  }

  /**
   * Like {@link EnumSet#copyOf}, but does not explode when given an empty collection.
   */
  public static <T extends Enum<T>> EnumSet<T> newEnumSet(Class<T> elementClass, Iterable<T> source) {
    EnumSet<T> result = EnumSet.noneOf(elementClass);
    for (T t : source) {
      result.add(t);
    }
    return result;
  }

  public static <K extends Enum<K>, V> EnumMap<K,V> newEnumMap(Class<K> keyClass, Map<K,V> source) {
    EnumMap<K,V> result = new EnumMap<>(keyClass);
    result.putAll(source);
    return result;
  }
}
