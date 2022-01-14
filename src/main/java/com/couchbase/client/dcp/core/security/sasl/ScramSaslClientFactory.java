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
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link SaslClientFactory} supporting various SCRAM-SHA modes.
 *
 * <p>See Mode for information which are supported and how they map to their
 * network representation.</p>
 *
 * @since 1.2.5
 */
public class ScramSaslClientFactory implements SaslClientFactory {

  @Override
  public SaslClient createSaslClient(final String[] mechanisms, final String authorizationId,
                                     final String protocol, final String serverName,
                                     final Map<String, ?> props, final CallbackHandler cbh)
    throws SaslException {
    if (authorizationId != null) {
      throw new SaslException("AuthorizationId is not supported");
    }

    if (cbh == null) {
      throw new SaslException("Callback handler required");
    }

    final Optional<Mode> mode = Mode.strongestOf(mechanisms);
    if (!mode.isPresent()) {
      return null;
    }

    try {
      return new ScramSaslClient(mode.get(), cbh);
    } catch (NoSuchAlgorithmException e) {
      throw new SaslException("Selected algorithm not supported.", e);
    }
  }

  @Override
  public String[] getMechanismNames(final Map<String, ?> props) {
    return Stream.of(Mode.values()).map(Mode::mech).toArray(String[]::new);
  }

  /**
   * Contains all the supported modes.
   */
  enum Mode {
    SCRAM_SHA512("SCRAM-SHA512"),
    SCRAM_SHA256("SCRAM-SHA256"),
    SCRAM_SHA1("SCRAM-SHA1");

    private final String mech;

    Mode(final String mech) {
      this.mech = mech;
    }

    /**
     * Returns the string/wire representation of the mechanism.
     */
    public String mech() {
      return mech;
    }

    /**
     * Takes the raw mech representation and returns its enum.
     *
     * @param mech the mech to check.
     * @return returns the mode if set or absent if not found.
     */
    static Optional<Mode> from(final String mech) {
      if (SCRAM_SHA1.mech().equalsIgnoreCase(mech)) {
        return Optional.of(SCRAM_SHA1);
      } else if (SCRAM_SHA256.mech().equalsIgnoreCase(mech)) {
        return Optional.of(SCRAM_SHA256);
      } else if (SCRAM_SHA512.mech().equalsIgnoreCase(mech)) {
        return Optional.of(SCRAM_SHA512);
      }

      return Optional.empty();
    }

    /**
     * Returns the strongest mechanism from the selected ones or
     * absent if none found in the list.
     *
     * @param mechs the mechs to check
     * @return the strongest mechanism from the selected ones or
     *         absent if none found in the list.
     */
    static Optional<Mode> strongestOf(final String[] mechs) {
      Set<Mode> foundModes = Stream
        .of(mechs)
        .map(Mode::from)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());

      if (foundModes.isEmpty()) {
        return Optional.empty();
      }

      if (foundModes.contains(Mode.SCRAM_SHA512)) {
        return Optional.of(Mode.SCRAM_SHA512);
      } else if (foundModes.contains(Mode.SCRAM_SHA256)) {
        return Optional.of(Mode.SCRAM_SHA256);
      } else {
        return Optional.of(Mode.SCRAM_SHA1);
      }
    }

  }
}
