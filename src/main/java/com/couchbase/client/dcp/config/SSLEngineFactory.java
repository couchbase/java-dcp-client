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
package com.couchbase.client.dcp.config;

import com.couchbase.client.core.endpoint.SSLException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Creates a {@link SSLEngine} which will be passed into the handler if SSL is enabled.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SSLEngineFactory {

    /**
     * The global environment which is shared.
     */
    private final SecureEnvironment env;

    /**
     * Create a new engine factory.
     *
     * @param env the config environment.
     */
    public SSLEngineFactory(SecureEnvironment env) {
        this.env = env;
    }

    /**
     * Returns a new {@link SSLEngine} constructed from the config settings.
     *
     * @return a {@link SSLEngine} ready to be used.
     */
    public SSLEngine get() {
        try {
            String pass = env.sslKeystorePassword();
            char[] password = pass == null || pass.isEmpty() ? null : pass.toCharArray();

            KeyStore ks = env.sslKeystore();
            if (ks == null) {
                ks = KeyStore.getInstance(KeyStore.getDefaultType());
                String ksFile = env.sslKeystoreFile();
                if (ksFile == null || ksFile.isEmpty()) {
                    throw new IllegalArgumentException("Path to Keystore File must not be null or empty.");
                }
                ks.load(new FileInputStream(ksFile), password);
            }
            String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(defaultAlgorithm);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(defaultAlgorithm);
            kmf.init(ks, password);
            tmf.init(ks);

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            SSLEngine engine = ctx.createSSLEngine();
            engine.setUseClientMode(true);
            return engine;
        } catch (Exception ex) {
            throw new SSLException("Could not create SSLEngine.", ex);
        }
    }
}
