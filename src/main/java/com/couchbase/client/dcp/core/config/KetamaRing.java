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

package com.couchbase.client.dcp.core.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.service.ServiceType;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static com.couchbase.client.core.util.CbCollections.filter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableNavigableMap;
import static java.util.Objects.requireNonNull;

/**
 * A <a href="https://www.metabrew.com/article/libketama-consistent-hashing-algo-memcached-clients">
 * Ketama ring
 * </a> (or "continuum", if you prefer) with Couchbase-specific parameters defined by
 * <a href="https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0026-ketama-hashing.md">
 * Couchbase SDK RFC 26 (Ketama Hashing)
 * </a>
 *
 * @param <E> The type of values in the ring.
 */
@Stability.Internal
public final class KetamaRing<E> {
  private final NavigableMap<Long, E> points;

  public static KetamaRing<NodeInfo> create(
      List<NodeInfo> nodes,
      MemcachedHashingStrategy strategy
  ) {
    List<NodeInfo> kvNodes = filter(nodes, it -> it.has(ServiceType.KV));

    // Compatibility note: libcouchbase sorts the nodes using a lexical sort on "host:port"
    // to get deterministic behavior if node hashes collide. Although deterministic, the actual
    // behavior is unclear, and might not be as simple as "first node wins" or "last node wins".
    //
    // The Java SDK does not sort the list.

    return new KetamaRing<>(kvNodes, strategy::hash);
  }

  /**
   * @param values The values to store in the ring (typically server addresses).
   * Order matters, because "last value wins" for hash collisions when populating the ring.
   * @param hashingStrategy Accepts a value and a counter token; returns a string
   * to use as input to the MD5 hash function when calculating Ketama points for a value.
   */
  // Visible for testing
  KetamaRing(List<E> values, BiFunction<E, Integer, String> hashingStrategy) {
    if (values.isEmpty()) {
      throw new IllegalArgumentException("Ketama ring must have at least one value.");
    }

    NavigableMap<Long, E> map = new TreeMap<>();

    MessageDigest md5 = newMd5Digest(); // Reusing the instance saves a bit of time.

    for (E value : values) {
      requireNonNull(value, "Ketama ring values must be non-null");

      // Assign this value to 160 (that's 40 * 4) points on the ring.
      for (int i = 0; i < 40; i++) {
        String valueIdentifier = hashingStrategy.apply(value, i);
        byte[] md5Hash = md5.digest(valueIdentifier.getBytes(UTF_8));
        ByteBuf md5HashBuf = Unpooled.wrappedBuffer(md5Hash);

        // MD5 hash is 16 bytes. Split it into 4 unsigned 32-bit ints.
        // We just got 4 hashes for the price of one!
        while (md5HashBuf.isReadable()) {
          long ketamaPoint = md5HashBuf.readUnsignedIntLE();
          map.put(ketamaPoint, value);
        }
      }
    }

    // Sanity check.
    requireUint32(map.firstKey());
    requireUint32(map.lastKey());

    this.points = unmodifiableNavigableMap(map);
  }

  /**
   * Returns the value associated with the key.
   */
  public E get(byte[] key) {
    // Ketama hash is the first 4 bytes of the MD5 hash, interpreted as a 32-bit little-endian unsigned int.
    long ketamaHash = Unpooled.wrappedBuffer(md5(key)).readUnsignedIntLE();
    return get(ketamaHash);
  }

  // Visible for testing
  E get(long ketamaHash) {
    requireUint32(ketamaHash);

    Map.Entry<Long, E> e = points.ceilingEntry(ketamaHash);
    return e != null
        ? e.getValue()
        : points.firstEntry().getValue(); // wrap around
  }

  // Visible for testing
  NavigableMap<Long, E> toMap() {
    return points;
  }

  private static void requireUint32(long v) {
    if (v < 0 || v > 0xffffffffL) {
      throw new IllegalArgumentException("Expected a value that fits in an unsigned 32-bit integer, but got: " + v);
    }
  }

  private static MessageDigest newMd5Digest() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not calculate MD5 hash.", e);
    }
  }

  private static byte[] md5(byte[] message) {
    return newMd5Digest().digest(message);
  }

}
