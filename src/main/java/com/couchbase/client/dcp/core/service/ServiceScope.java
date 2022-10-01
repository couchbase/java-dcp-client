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

package com.couchbase.client.dcp.core.service;

public enum ServiceScope {

  /**
   * The service is able to handle all buckets at the same time.
   * <p>
   * This is true for all services where their authentication mechanism
   * is not bound to the connection, but rather to the request object itself.
   */
  CLUSTER,

  /**
   * The service can only handle one bucket at a time.
   * <p>
   * This is true for all services which have their authentication mechanism
   * bound at connection time, allowing a service not to be reused across buckets.
   */
  BUCKET

}
