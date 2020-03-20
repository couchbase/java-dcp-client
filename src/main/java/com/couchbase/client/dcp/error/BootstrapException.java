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
package com.couchbase.client.dcp.error;

import com.couchbase.client.dcp.core.CouchbaseException;

/**
 * This exception indicates an error during bootstrap. See the cause and message for more details.
 */
public class BootstrapException extends CouchbaseException {

  public BootstrapException() {
  }

  public BootstrapException(String message) {
    super(message);
  }

  public BootstrapException(String message, Throwable cause) {
    super(message, cause);
  }

  public BootstrapException(Throwable cause) {
    super(cause);
  }
}
