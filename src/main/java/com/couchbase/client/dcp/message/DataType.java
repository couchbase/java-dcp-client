/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.message;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#data-types
 */
public enum DataType {
  JSON(0x01),
  SNAPPY(0x02),
  XATTR(0x04);

  private final int bitmask;

  private static final List<DataType> values = unmodifiableList(Arrays.asList(values()));

  DataType(int bitmask) {
    this.bitmask = bitmask;
  }

  public int bitmask() {
    return bitmask;
  }

  public static EnumSet<DataType> parse(int bitfield) {
    EnumSet<DataType> result = EnumSet.noneOf(DataType.class);
    for (DataType type : values) {
      if (contains(bitfield, type)) {
        result.add(type);
      }
    }
    return result;
  }

  public static boolean contains(int bitfield, DataType type) {
    return (bitfield & type.bitmask()) != 0;
  }
}
