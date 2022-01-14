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

package com.couchbase.client.dcp.core.security.sasl;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.util.Map;

/**
 * This {@link SaslClientFactory} supports all couchbase supported SASL authentication
 * mechanisms.
 *
 * <p>It first tries to establish a SCRAM-SHA*-based sasl client, but if the mechanism is not
 * supported then it will fall back to the JVM-implemented one which supports the rest (i.e.
 * PLAIN and CRAM-MD5).</p>
 *
 * @since 2.0.0
 */
public class CouchbaseSaslClientFactory implements SaslClientFactory {

  private static final SaslClientFactory SCRAM_FACTORY = new ScramSaslClientFactory();

  @Override
  public SaslClient createSaslClient(final String[] mechanisms, final String authorizationId,
                                     final String protocol, final String serverName,
                                     final Map<String, ?> props, final CallbackHandler cbh)
    throws SaslException {

    SaslClient client = SCRAM_FACTORY
      .createSaslClient(mechanisms, authorizationId, protocol, serverName, props, cbh);
    if (client == null) {
      client = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, cbh);
    }
    return client;
  }

  /**
   * Note that this method should be never used, but for completeness sake it
   * returns all supported mechanisms by Couchbase.
   *
   * <p>The actual selection happens somewhere else and is not bound to this list.</p>
   *
   * @param props the properties, ignored here.
   * @return all mechanisms couchbase supports.
   */
  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return new String[] {
      ScramSaslClientFactory.Mode.SCRAM_SHA512.mech(),
      ScramSaslClientFactory.Mode.SCRAM_SHA256.mech(),
      ScramSaslClientFactory.Mode.SCRAM_SHA1.mech(),
      "CRAM-MD5",
      "PLAIN"
    };
  }
}
