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

package com.couchbase.client.dcp.message;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Code describing why producer decided to close the stream.
 */
public class StreamEndReason {
  private static final StreamEndReason[] values = new StreamEndReason[256];

  public static final StreamEndReason OK = new StreamEndReason(0, "OK", "The stream has finished without error.");
  public static final StreamEndReason CLOSED = new StreamEndReason(1, "CLOSED", "The close stream command was invoked on this stream causing it to be closed by force.");
  public static final StreamEndReason STATE_CHANGED = new StreamEndReason(2, "STATE_CHANGED", "The state of the VBucket that is being streamed has changed to state that the consumer does not want to receive.");
  public static final StreamEndReason DISCONNECTED = new StreamEndReason(3, "DISCONNECTED", "The stream is closed because the connection was disconnected.");
  public static final StreamEndReason TOO_SLOW = new StreamEndReason(4, "TOO_SLOW", "The stream is closing because the client cannot read from the stream fast enough." +
      " This is done to prevent the server from running out of resources trying while" +
      " trying to serve the client. When the client is ready to read from the stream" +
      " again it should reconnect.");
  public static final StreamEndReason BACKFILL_FAILED = new StreamEndReason(5, "BACKFILL_FAILED", "The stream is closed because the backfill failed.");
  public static final StreamEndReason ROLLBACK = new StreamEndReason(6, "ROLLBACK", "The stream closed because the vbucket is rolling back. Client must reopen stream and follow rollback protocol.");
  public static final StreamEndReason FILTER_EMPTY = new StreamEndReason(7, "FILTER_EMPTY", "The stream closed because all filtered collections or scope are now deleted and no more data is coming.");
  public static final StreamEndReason LOST_PRIVILEGES = new StreamEndReason(8, "LOST_PRIVILEGES", "The stream closed because the connection no longer has the required access for the stream's configuration (e.g. no DcpStream on a filtered collection).");

  private final int value;
  private final String name;
  private final String description;

  private StreamEndReason(int value, String name, String description) {
    this.value = value;
    this.name = requireNonNull(name);
    this.description = requireNonNull(description);
    if (values[value] != null) {
      throw new IllegalStateException("already initialized stream end reason " + values[value]);
    }
    values[value] = this;
  }

  private StreamEndReason(int value) {
    // don't delegate to other c'tor, because we don't want this ending up in the cached value map.
    this.value = value;
    this.name = String.valueOf(value);
    this.description = "The stream closed with reason code " + value + " (which this client does not recognize).";
  }

  public int value() {
    return value;
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  static StreamEndReason of(int value) {
    StreamEndReason reason = null;
    if (value >= 0 && value < values.length) {
      reason = values[value]; // might be undefined, so can't return right away
    }
    return reason == null ? new StreamEndReason(value) : reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamEndReason that = (StreamEndReason) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return name;
  }
}
