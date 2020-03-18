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
package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class is used during bootstrap to configure all the possible DCP negotiation parameters.
 */
public class DcpControl {
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpControl.class);

  /**
   * NOOP interval to use when none is specified.
   */
  private static final int DEFAULT_NOOP_INTERVAL_SECONDS = 120;

  /**
   * Stores the params to negotiate in a map.
   */
  private Map<String, String> values;

  /**
   * The requested compression mode.
   */
  private CompressionMode compressionMode = CompressionMode.ENABLED;

  /**
   * Creates a new {@link DcpControl} instance with no params set upfront.
   */
  public DcpControl() {
    this.values = new HashMap<>();
  }

  /**
   * Set the compression mode to use.
   * If not specified, defaults to {@link CompressionMode#DISABLED}.
   */
  public void compression(CompressionMode compressionMode) {
    this.compressionMode = requireNonNull(compressionMode);
  }

  /**
   * Returns the requested compression mode, or the fallback mode if the
   * server does not support the requested mode.
   */
  public CompressionMode compression(Version version) {
    return compressionMode.effectiveMode(version);
  }

  /**
   * Store/Override a control parameter.
   *
   * @param name the name of the control parameter.
   * @param value the stringified version what it should be set to.
   * @return the {@link DcpControl} instance for chainability.
   */
  public DcpControl put(final Names name, final String value) {
    // Provide a default NOOP interval because the client needs
    // to know the interval in order to detect dead connections.
    if (name == Names.ENABLE_NOOP && get(Names.SET_NOOP_INTERVAL) == null) {
      put(Names.SET_NOOP_INTERVAL, Integer.toString(DEFAULT_NOOP_INTERVAL_SECONDS));
    }

    values.put(name.value(), value);
    return this;
  }

  /**
   * Returns a param if set, otherwise null is returned.
   *
   * @param name the name of the param.
   * @return the stringified value if set, null otherwise.
   */
  public String get(final Names name) {
    return values.get(name.value());
  }

  /**
   * Shorthand getter to check if buffer acknowledgements are enabled.
   */
  public boolean bufferAckEnabled() {
    String bufSize = get(Names.CONNECTION_BUFFER_SIZE);
    return bufSize != null && Integer.parseInt(bufSize) > 0;
  }

  /**
   * Shorthand getter to check if noops are enabled.
   */
  public boolean noopEnabled() {
    return "true".equals(get(Names.ENABLE_NOOP));
  }

  /**
   * Shorthand getter for the NOOP interval.
   */
  public int noopIntervalSeconds() {
    try {
      return parsePositiveInteger(get(Names.SET_NOOP_INTERVAL));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Bad value for '" + Names.SET_NOOP_INTERVAL.value() + "'; " + e.getMessage());
    }
  }

  private static int parsePositiveInteger(String s) {
    int value = Integer.parseInt(s);
    if (value < 1) {
      throw new IllegalArgumentException("Value '" + s + "' is not positive.");
    }
    return value;
  }

  /**
   * Returns the control settings, adjusted for the actual Couchbase Server version.
   */
  public Map<String, String> getControls(Version serverVersion) {
    final CompressionMode effectiveMode = compression(serverVersion);

    if (compressionMode != effectiveMode) {
      LOGGER.info("Couchbase Server version {} does not support {} compression mode; falling back to {}.",
          serverVersion, compressionMode, effectiveMode);
    } else {
      LOGGER.debug("Compression mode: {}", compressionMode);
    }

    final Map<String, String> result = new HashMap<>(values);
    result.putAll(effectiveMode.getDcpControls(serverVersion));
    return result;
  }

  /**
   * All the possible control options available.
   * <p>
   * Note that not all params might be supported by the used server version, see the
   * description for each for more information.
   */
  public enum Names {
    /**
     * Used to enable to tell the Producer that the Consumer supports detecting
     * dead connections through the use of a noop. The value for this message should
     * be set to either "true" or "false". See the page on dead connections for more
     * details. This parameter is available starting in Couchbase 3.0.
     */
    ENABLE_NOOP("enable_noop"),
    /**
     * Used to tell the Producer the size of the Consumer side buffer in bytes which
     * the Consumer is using for flow control. The value of this parameter should be
     * an integer in string form between 1 and 2^32. See the page on flow control for
     * more details. This parameter is available starting in Couchbase 3.0.
     */
    CONNECTION_BUFFER_SIZE("connection_buffer_size"),
    /**
     * Sets the noop interval on the Producer. Values for this parameter should be
     * an integer in string form between 20 and 10800. This allows the noop interval
     * to be set between 20 seconds and 3 hours. This parameter should always be set
     * when enabling noops to prevent the Consumer and Producer having a different
     * noop interval. This parameter is available starting in Couchbase 3.0.1.
     */
    SET_NOOP_INTERVAL("set_noop_interval"),
    /**
     * Sets the priority that the connection should have when sending data.
     * The priority may be set to "high", "medium", or "low". High priority connections
     * will send messages at a higher rate than medium and low priority connections.
     * This parameter is availale starting in Couchbase 4.0.
     */
    SET_PRIORITY("set_priority"),
    /**
     * Enables sending extended meta data. This meta data is mainly used for internal
     * server processes and will vary between different releases of Couchbase. See
     * the documentation on extended meta data for more information on what will be
     * sent. Each version of Couchbase will support a specific version of extended
     * meta data. This parameter is available starting in Couchbase 4.0.
     */
    ENABLE_EXT_METADATA("enable_ext_metadata"),
    /**
     * Compresses values using snappy compression before sending them. This parameter
     * is available starting in Couchbase 4.5.
     *
     * @deprecated in favor of specifying compression settings by calling
     * {@link #compression(CompressionMode)}. Scheduled for removal in next major version.
     */
    @Deprecated
    ENABLE_VALUE_COMPRESSION("enable_value_compression"),
    /**
     * Tells the server that the client can tolerate the server dropping the connection.
     * The server will only do this if the client cannot read data from the stream fast
     * enough and it is highly recommended to be used in all situations. We only support
     * disabling cursor dropping for backwards compatibility. This parameter is available
     * starting in Couchbase 4.5.
     */
    SUPPORTS_CURSOR_DROPPING("supports_cursor_dropping");

    private String value;

    Names(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  @Override
  public String toString() {
    return "DcpControl{" + values + '}';
  }
}
