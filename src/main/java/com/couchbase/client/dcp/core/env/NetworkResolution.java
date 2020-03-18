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

package com.couchbase.client.dcp.core.env;

public class NetworkResolution {

  /**
   * Pick whatever the server returns in the config, this is the
   * old and backwards compatible mode (server default).
   */
  public static NetworkResolution DEFAULT = new NetworkResolution("default");

  /**
   * Based on heuristics discovers if internal or
   * external resolution will be used.
   *
   * This is the default setting (not to be confused with
   * the default mode)!
   */
  public static NetworkResolution AUTO = new NetworkResolution("auto");

  /**
   * Pins it to external resolution.
   */
  public static NetworkResolution EXTERNAL = new NetworkResolution("external");

  /**
   * Stores the internal name.
   */
  private final String name;

  /**
   * Provide a network resolution option which is not covered by the statics defined
   * in this class.
   *
   * @param name the name to use.
   * @return a new {@link NetworkResolution}.
   */
  public static NetworkResolution custom(final String name) {
    return new NetworkResolution(name);
  }

  /**
   * Creates a new network resolution option.
   */
  private NetworkResolution(final String name) {
    this.name = name;
  }

  /**
   * Returns the wire representation of the network resolution setting.
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NetworkResolution that = (NetworkResolution) o;

    return name != null ? name.equals(that.name) : that.name == null;
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "NetworkResolution{" +
        "name='" + name + '\'' +
        '}';
  }
}
