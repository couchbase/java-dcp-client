/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.metrics;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;

class DcpMetricsHelper {
  static void log(CouchbaseLogger logger, CouchbaseLogLevel level, String message, Object param) {
    if (level != null) {
      logger.log(level, message, param);
    }
  }

  static void log(CouchbaseLogger logger, CouchbaseLogLevel level, String message, Object param1, Object param2) {
    if (level != null) {
      logger.log(level, message, param1, param2);
    }
  }
}
