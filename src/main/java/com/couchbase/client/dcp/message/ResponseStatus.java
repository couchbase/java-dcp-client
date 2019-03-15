/*
 * Copyright 2018 Couchbase, Inc.
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

/**
 * Response status codes and messages, as defined in the
 * <a href="https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md">
 * Couchbase Binary Protocol specification</a>.
 */
public class ResponseStatus {
  private static final ResponseStatus[] values = new ResponseStatus[256];

  public static final ResponseStatus NO_ERROR = new ResponseStatus(0x0000, "No error");
  public static final ResponseStatus KEY_NOT_FOUND = new ResponseStatus(0x0001, "Key not found");
  public static final ResponseStatus KEY_EXISTS = new ResponseStatus(0x0002, "Key exists");
  public static final ResponseStatus VALUE_TOO_LARGE = new ResponseStatus(0x0003, "Value too large");
  public static final ResponseStatus INVALID_ARGUMENTS = new ResponseStatus(0x0004, "Invalid arguments");
  public static final ResponseStatus ITEM_NOT_STORED = new ResponseStatus(0x0005, "Item not stored");
  public static final ResponseStatus NON_NUMERIC = new ResponseStatus(0x0006, "Incr/Decr on a non-numeric value");
  public static final ResponseStatus NOT_MY_VBUCKET = new ResponseStatus(0x0007, "The vbucket belongs to another server");
  public static final ResponseStatus NOT_CONNECTED_TO_BUCKET = new ResponseStatus(0x0008, "The connection is not connected to a bucket");
  public static final ResponseStatus STALE_AUTH_CONTEXT = new ResponseStatus(0x0001f, "The authentication context is stale, please re-authenticate");
  public static final ResponseStatus AUTH_ERROR = new ResponseStatus(0x0020, "Authentication error");
  public static final ResponseStatus AUTH_CONTINUE = new ResponseStatus(0x0021, "Authentication continue");
  public static final ResponseStatus ILLEGAL_RANGE = new ResponseStatus(0x0022, "The requested value is outside the legal ranges");
  public static final ResponseStatus ROLLBACK_REQUIRED = new ResponseStatus(0x0023, "Rollback required");
  public static final ResponseStatus NO_ACCESS = new ResponseStatus(0x0024, "No access / insufficient permissions");
  public static final ResponseStatus INITIALIZING_NODE = new ResponseStatus(0x0025, "The node is being initialized");
  public static final ResponseStatus UNKNOWN_COMMAND = new ResponseStatus(0x0081, "Unknown command");
  public static final ResponseStatus OUT_OF_MEMORY = new ResponseStatus(0x0082, "Out of memory");
  public static final ResponseStatus NOT_SUPPORTED = new ResponseStatus(0x0083, "Not supported");
  public static final ResponseStatus INTERNAL_ERROR = new ResponseStatus(0x0084, "Internal error");
  public static final ResponseStatus BUSY = new ResponseStatus(0x0085, "Busy");
  public static final ResponseStatus TEMPORARY_FAILURE = new ResponseStatus(0x0086, "Temporary Failure");

  /**
   * Returns the ResponseStatus with the given status code. For recognized codes, this method
   * is guaranteed to always return the same ResponseStatus instance, so it's safe to use == for equality checks
   * against the pre-defined constants.
   */
  public static ResponseStatus valueOf(int code) {
    code = code & 0xFFFF; // convert negative shorts to unsigned
    if (code < 256) {
      ResponseStatus status = values[code];
      if (status != null) {
        return status;
      }
    }
    return new ResponseStatus(code);
  }

  private final int code;
  private final String formatted;

  /**
   * Created a ResponseStatus with a recognized code.
   */
  private ResponseStatus(int code, String message) {
    this.code = code;
    this.formatted = format(code, message);

    if (values[code] != null) {
      throw new IllegalStateException("already initialized status " + values[code]);
    }

    values[code] = this;
  }

  /**
   * Created a ResponseStatus with an unrecognized code.
   */
  private ResponseStatus(int code) {
    this.code = code;
    this.formatted = format(code, "???");
  }

  private static String format(int code, String message) {
    return String.format("0x%04x (%s)", code, message);
  }

  /**
   * @return the status code as an unsigned value.
   */
  public int code() {
    return code;
  }

  public boolean isTemporary() {
    return this == BUSY || this == TEMPORARY_FAILURE;
  }

  public boolean isSuccess() {
    return this == NO_ERROR;
  }

  public String toString() {
    return formatted;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ResponseStatus that = (ResponseStatus) o;

    return code == that.code;
  }

  @Override
  public int hashCode() {
    return code;
  }
}
