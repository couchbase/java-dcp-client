/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.dcp.core.config;

import java.util.Arrays;

public class DefaultPartition implements Partition {

  private final short master;
  private final short[] replicas;

  /**
   * Creates a new {@link DefaultPartition}.
   *
   * @param master the array index of the master
   * @param replicas the array indexes of the replicas.
   */
  public DefaultPartition(short master, short[] replicas) {
    this.master = master;
    this.replicas = replicas;
  }

  @Override
  public short master() {
    return master;
  }

  @Override
  public short replica(int num) {
    if (num >= replicas.length) {
      return -2;
    }
    return replicas[num];
  }

  @Override
  public String toString() {
    return "[m: " + master + ", r: " + Arrays.toString(replicas) + "]";
  }
}
