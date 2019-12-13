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
package com.couchbase.client.dcp;

/**
 * A generic interface which determines the name for a DCP connection.
 * <p>
 * Adhering to the semantics of DCP, it is very important that the names of the connection, if independent
 * also need to be different. Otherwise the server will inevitably close the old connection, leading
 * to weird edge cases. Keep this in mind when implementing the interface or just stick with the
 * default {@link DefaultConnectionNameGenerator}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public interface ConnectionNameGenerator {

  /**
   * Generate the name for a DCP Connection.
   * <p>
   * The name must be no longer than 200 bytes when converted to UTF-8.
   */
  String name();
}
