/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.perfrunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class GenericUri {
  // From https://tools.ietf.org/html/rfc3986#appendix-B
  private static final Pattern URI_PATTERN = Pattern.compile(
      "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

  private final String scheme;
  private final String authority;
  private final String path;
  private final String query;
  private final String fragment;

  GenericUri(String uri) throws IllegalArgumentException {
    Matcher m = URI_PATTERN.matcher(uri);
    if (!m.matches()) {
      throw new IllegalArgumentException("Failed to parse URI: " + uri);
    }
    this.scheme = m.group(2);
    this.authority = m.group(4);
    this.path = m.group(5);
    this.query = m.group(7);
    this.fragment = m.group(9);
  }

  public String scheme() {
    return scheme;
  }

  public String authority() {
    return authority;
  }

  public String path() {
    return path;
  }

  public String query() {
    return query;
  }

  public String fragment() {
    return fragment;
  }
}
