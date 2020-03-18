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
package com.couchbase.client.dcp.core.security.sasl;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.Map;

/**
 * A wrapper for {@link javax.security.sasl.Sasl} which first tries the mechanisms that are supported
 * out of the box and then tries our extended range (provided by {@link ShaSaslClient}.
 */
public class Sasl {

  /**
   * Our custom client factory which supports the additional mechanisms.
   */
  private static ShaSaslClientFactory SASL_FACTORY = new ShaSaslClientFactory();

  /**
   * Creates a new {@link SaslClient} and first tries the JVM built in clients before falling back
   * to our custom implementations. The mechanisms are tried in the order they arrive.
   */
  public static SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
                                            String serverName, Map<String, ?> props, CallbackHandler cbh) throws SaslException {

    boolean enableScram = Boolean.parseBoolean(System.getProperty("com.couchbase.scramEnabled", "true"));

    for (String mech : mechanisms) {
      String[] mechs = new String[]{mech};

      SaslClient client = javax.security.sasl.Sasl.createSaslClient(
          mechs, authorizationId, protocol, serverName, props, cbh
      );

      if (client == null && enableScram) {
        client = SASL_FACTORY.createSaslClient(
            mechs, authorizationId, protocol, serverName, props, cbh
        );
      }

      if (client != null) {
        return client;
      }
    }

    return null;
  }
}
