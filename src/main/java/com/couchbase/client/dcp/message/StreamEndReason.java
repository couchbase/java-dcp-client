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

/**
 * Code describing why producer decided to close the stream.
 */
public enum StreamEndReason {
  /**
   * The stream has finished without error.
   */
  OK(0x00),
  /**
   * The close stream command was invoked on this stream causing it to be closed
   * by force.
   */
  CLOSED(0x01),
  /**
   * The state of the VBucket that is being streamed has changed to state that
   * the consumer does not want to receive.
   */
  STATE_CHANGED(0x02),
  /**
   * The stream is closed because the connection was disconnected.
   */
  DISCONNECTED(0x03),
  /**
   * The stream is closing because the client cannot read from the stream fast enough.
   * This is done to prevent the server from running out of resources trying while
   * trying to serve the client. When the client is ready to read from the stream
   * again it should reconnect. This flag is available starting in Couchbase 4.5.
   */
  TOO_SLOW(0x04);

  private final int value;

  StreamEndReason(int value) {
    this.value = value;
  }

  public int value() {
    return value;
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
      default:
        throw new IllegalArgumentException("Unknown stream end reason: " + value);
    }
  }

}
