/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.dcp.core.logging;

/**
 * Represents a logging argument which is subject to redaction.
 */
public class RedactableArgument {

  /**
   * The type of the redactable argument.
   */
  private final ArgumentType type;

  /**
   * The message of the redactable argument.
   */
  private final Object message;

  /**
   * Creates a new {@link RedactableArgument}.
   *
   * @param type type of the redactable argument.
   * @param message message of the redactable argument.
   */
  private RedactableArgument(final ArgumentType type, final Object message) {
    this.type = type;
    this.message = message;
  }

  /**
   * A redactable argument of user data. User data is data that is stored into Couchbase
   * by the application user account, including:
   * <ul>
   * <li>Key and value pairs in JSON documents, or the key exclusively
   * <li>Application/Admin usernames that identify the human person
   * <li>Query statements included in the log file collected by support that leak the document fields (Select floor_price from stock).
   * <li>Names and email addresses asked during product registration and alerting
   * <li>Usernames
   * <li>Document xattrs
   * </ul>
   *
   * @param message the message to redact.
   * @return a new {@link RedactableArgument}.
   */
  public static RedactableArgument redactUser(final Object message) {
    return new RedactableArgument(ArgumentType.USER, message);
  }

  /**
   * @deprecated Please use {@link #redactUser(Object)} instead.
   */
  @Deprecated
  public static RedactableArgument user(final Object message) {
    return redactUser(message);
  }

  /**
   * A redactable argument of meta data. Metadata is logical data needed by Couchbase
   * to store and process User data, including:
   * <ul>
   * <li>Cluster name
   * <li>Bucket names
   * <li>DDoc/view names
   * <li>View code
   * <li>Index names
   * <li>Mapreduce Design Doc Name and Definition (IP)
   * <li>XDCR Replication Stream Names
   * <li>And other couchbase resource specific meta data
   * </ul>
   *
   * @param message the message to redact.
   * @return a new {@link RedactableArgument}.
   */
  public static RedactableArgument redactMeta(final Object message) {
    return new RedactableArgument(ArgumentType.META, message);
  }

  /**
   * @deprecated Please use {@link #redactMeta(Object)} instead.
   */
  @Deprecated
  public static RedactableArgument meta(final Object message) {
    return redactMeta(message);
  }

  /**
   * A redactable argument of system data. System data is data from other parts of the system
   * Couchbase interacts with over the network, including:
   * <ul>
   * <li>IP addresses
   * <li>IP tables
   * <li>Hosts names
   * <li>Ports
   * <li>DNS topology
   * </ul>
   *
   * @param message the message to redact.
   * @return a new {@link RedactableArgument}.
   */
  public static RedactableArgument redactSystem(final Object message) {
    return new RedactableArgument(ArgumentType.SYSTEM, message);
  }

  /**
   * @deprecated Please use {@link #redactSystem(Object)} instead.
   */
  @Deprecated
  public static RedactableArgument system(final Object message) {
    return redactSystem(message);
  }

  /**
   * The type of this redactable argument.
   */
  public ArgumentType type() {
    return type;
  }

  /**
   * The message of this redactable argument.
   */
  public String message() {
    return String.valueOf(message);
  }

  @Override
  public String toString() {
    // The exact syntax for "system" and "meta" redaction is yet to be determined.
    // In the meantime, we've been asked to redact *only* "user" data.
    if (type != ArgumentType.USER) {
      return message();
    }

    final RedactionLevel redactionLevel = RedactionLevel.get();

    final boolean redact;
    switch (redactionLevel) {
      case NONE:
        redact = false;
        break;
      case PARTIAL:
        redact = (type == ArgumentType.USER);
        break;
      case FULL:
        redact = true;
        break;
      default:
        throw new AssertionError("Unexpected redaction level: " + redactionLevel);
    }

    return redact ? "<" + type.tagName + ">" + message() + "</" + type.tagName + ">" : message();
  }

  /**
   * The type of the redactable argument.
   */
  public enum ArgumentType {
    /**
     * User data is data that is stored into Couchbase by
     * the application user account.
     */
    USER("ud"),

    /**
     * Metadata is logical data needed by Couchbase to
     * store and process user data.
     */
    META("md"),

    /**
     * System data is data from other parts of the system
     * Couchbase interacts with over the network.
     */
    SYSTEM("sd");

    private final String tagName;

    ArgumentType(String tagName) {
      this.tagName = tagName;
    }
  }

}
