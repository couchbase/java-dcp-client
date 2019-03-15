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

package com.couchbase.client.dcp;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ClientVersion {

  private static final String CLIENT_VERSION = loadClientVersion();

  private static String loadClientVersion() {
    try (InputStream is = ClientVersion.class.getResourceAsStream("version.properties");
         Reader r = new InputStreamReader(is, UTF_8)) {

      Properties props = new Properties();
      props.load(r);

      return (String) props.getOrDefault("projectVersion", "unknown");

    } catch (Throwable t) {
      t.printStackTrace();
      return "unknown";
    }
  }

  public static String clientVersion() {
    return CLIENT_VERSION;
  }
}
