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
  private final int value;
  private final String name;
  private final String description;

  private StreamEndReason(int value, String name, String description) {
    this.value = value;
    this.name = requireNonNull(name);
    this.description = requireNonNull(description);
  }

  public static final StreamEndReason OK = new StreamEndReason(0, "OK", "The stream has finished without error.");
  public static final StreamEndReason CLOSED = new StreamEndReason(1, "CLOSED", "The close stream command was invoked on this stream causing it to be closed by force.");
  public static final StreamEndReason STATE_CHANGED = new StreamEndReason(2, "STATE_CHANGED", "The state of the VBucket that is being streamed has changed to state that the consumer does not want to receive.");
  public static final StreamEndReason DISCONNECTED = new StreamEndReason(3, "DISCONNECTED", "The stream is closed because the connection was disconnected.");
  public static final StreamEndReason TOO_SLOW = new StreamEndReason(4, "TOO_SLOW", "The stream is closing because the client cannot read from the stream fast enough." +
      " This is done to prevent the server from running out of resources trying while" +
      " trying to serve the client. When the client is ready to read from the stream" +
      " again it should reconnect. This flag is available starting in Couchbase 4.5.");
  public static final StreamEndReason BACKFILL_FAILED = new StreamEndReason(5, "BACKFILL_FAILED", "The stream is closed because the backfill failed");

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
    switch (value) {
      case 0x00:
        return OK;
      case 0x01:
        return CLOSED;
      case 0x02:
        return STATE_CHANGED;
      case 0x03:
        return DISCONNECTED;
      case 0x04:
        return TOO_SLOW;
      case 0x05:
        return BACKFILL_FAILED;
      default:
        return new StreamEndReason(value, Integer.toString(value),
            "Stream end reason " + value + " is not recognized by this DCP client.");
    }
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
