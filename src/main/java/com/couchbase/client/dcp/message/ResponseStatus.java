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

import static java.util.Objects.requireNonNull;

/**
 * Response status codes and messages, as defined in the
 * <a href="https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#response-status">
 * Response Status</a> section of the Couchbase Binary Protocol specification.
 */
public class ResponseStatus {
  private static final ResponseStatus[] values = new ResponseStatus[256];

  public static final ResponseStatus NO_ERROR = new ResponseStatus(0x0000, "NO_ERROR", "No error");
  public static final ResponseStatus KEY_NOT_FOUND = new ResponseStatus(0x0001, "KEY_NOT_FOUND", "Key not found");
  public static final ResponseStatus KEY_EXISTS = new ResponseStatus(0x0002, "KEY_EXISTS", "Key exists");
  public static final ResponseStatus VALUE_TOO_LARGE = new ResponseStatus(0x0003, "VALUE_TOO_LARGE", "Value too large");
  public static final ResponseStatus INVALID_ARGUMENTS = new ResponseStatus(0x0004, "INVALID_ARGUMENTS", "Invalid arguments");
  public static final ResponseStatus ITEM_NOT_STORED = new ResponseStatus(0x0005, "ITEM_NOT_STORED", "Item not stored");
  public static final ResponseStatus NON_NUMERIC = new ResponseStatus(0x0006, "NON_NUMERIC", "Incr/Decr on a non-numeric value");
  public static final ResponseStatus NOT_MY_VBUCKET = new ResponseStatus(0x0007, "NOT_MY_VBUCKET", "The vbucket belongs to another server");
  public static final ResponseStatus NOT_CONNECTED_TO_BUCKET = new ResponseStatus(0x0008, "NOT_CONNECTED_TO_BUCKET", "The connection is not connected to a bucket");
  public static final ResponseStatus STALE_AUTH_CONTEXT = new ResponseStatus(0x0001f, "STALE_AUTH_CONTEXT", "The authentication context is stale, please re-authenticate");
  public static final ResponseStatus AUTH_ERROR = new ResponseStatus(0x0020, "AUTH_ERROR", "Authentication error");
  public static final ResponseStatus AUTH_CONTINUE = new ResponseStatus(0x0021, "AUTH_CONTINUE", "Authentication continue");
  public static final ResponseStatus ILLEGAL_RANGE = new ResponseStatus(0x0022, "ILLEGAL_RANGE", "The requested value is outside the legal ranges");
  public static final ResponseStatus ROLLBACK_REQUIRED = new ResponseStatus(0x0023, "ROLLBACK_REQUIRED", "Rollback required");
  public static final ResponseStatus NO_ACCESS = new ResponseStatus(0x0024, "NO_ACCESS", "No access / insufficient permissions (or does not exist)");
  public static final ResponseStatus INITIALIZING_NODE = new ResponseStatus(0x0025, "INITIALIZING_NODE", "The node is being initialized");
  public static final ResponseStatus UNKNOWN_COMMAND = new ResponseStatus(0x0081, "UNKNOWN_COMMAND", "Unknown command");
  public static final ResponseStatus OUT_OF_MEMORY = new ResponseStatus(0x0082, "OUT_OF_MEMORY", "Out of memory");
  public static final ResponseStatus NOT_SUPPORTED = new ResponseStatus(0x0083, "NOT_SUPPORTED", "Not supported");
  public static final ResponseStatus INTERNAL_ERROR = new ResponseStatus(0x0084, "INTERNAL_ERROR", "Internal error");
  public static final ResponseStatus BUSY = new ResponseStatus(0x0085, "BUSY", "Busy");
  public static final ResponseStatus TEMPORARY_FAILURE = new ResponseStatus(0x0086, "TEMPORARY_FAILURE", "Temporary Failure");
  public static final ResponseStatus XATTR_INVALID_SYNTAX = new ResponseStatus(0x0087, "XATTR_INVALID_SYNTAX", "XATTR invalid syntax");
  public static final ResponseStatus UNKNOWN_COLLECTION = new ResponseStatus(0x0088, "UNKNOWN_COLLECTION", "Unknown collection");
  public static final ResponseStatus NO_COLLECTIONS_MANIFEST = new ResponseStatus(0x0089, "NO_COLLECTIONS_MANIFEST", "No collections manifest");
  public static final ResponseStatus COLLECTIONS_MANIFEST_NOT_APPLIED = new ResponseStatus(0x008a, "COLLECTIONS_MANIFEST_NOT_APPLIED", "Collections manifest not applied");
  public static final ResponseStatus CLIENT_COLLECTIONS_MANIFEST_AHEAD = new ResponseStatus(0x008b, "CLIENT_COLLECTIONS_MANIFEST_AHEAD", "Client collections manifest ahead");
  public static final ResponseStatus UNKNOWN_SCOPE = new ResponseStatus(0x008c, "UNKNOWN_SCOPE", "Unknown scope");
  public static final ResponseStatus DURABILITY_LEVEL_INVALID = new ResponseStatus(0x00a0, "DURABILITY_LEVEL_INVALID", "Durability level invalid");
  public static final ResponseStatus DURABILITY_IMPOSSIBLE = new ResponseStatus(0x00a1, "DURABILITY_IMPOSSIBLE", "Durability impossible");
  public static final ResponseStatus SYNCHRONOUS_WRITE_IN_PROGRESS = new ResponseStatus(0x00a2, "SYNCHRONOUS_WRITE_IN_PROGRESS", "Synchronous write in progress");
  public static final ResponseStatus SYNCHRONOUS_WRITE_AMBIGUOUS = new ResponseStatus(0x00a3, "SYNCHRONOUS_WRITE_AMBIGUOUS", "Synchronous write ambiguous");

  // Synthetic, never returned by server
  private static final int MALFORMED_RESPONSE_CODE = -1;
  public static final ResponseStatus MALFORMED_RESPONSE = new ResponseStatus(MALFORMED_RESPONSE_CODE, "MALFORMED_RESPONSE", "Malformed response");

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
  private final String symbolicName;

  /**
   * Created a ResponseStatus with a recognized code.
   */
  private ResponseStatus(int code, String symbolicName, String description) {
    this.code = code;
    this.formatted = format(code, description);
    this.symbolicName = requireNonNull(symbolicName);

    if (code == MALFORMED_RESPONSE_CODE) {
      // skip further processing of the "malformed response" status
      return;
    }

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
    this.symbolicName = String.format("0x%04x", code);
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

  public String formatted() {
    return formatted;
  }

  public String symbolicName() {
    return symbolicName;
  }

  public String toString() {
    return formatted();
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
