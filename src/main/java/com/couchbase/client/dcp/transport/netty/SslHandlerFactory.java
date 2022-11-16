/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.dcp.transport.netty;


import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.handler.ssl.OpenSsl;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslProvider;
import com.couchbase.client.dcp.Authenticator;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.dcp.config.HostAndPort;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.security.cert.X509Certificate;

/**
 * This factory creates {@link SslHandler} based on a given configuration.
 */
public class SslHandlerFactory {
  private SslHandlerFactory() {
    throw new AssertionError("not instantiable");
  }

  public static SslHandler get(final ByteBufAllocator allocator, final SecurityConfig config,
                               HostAndPort peer, Authenticator authenticator) throws Exception {
    SslProvider provider = OpenSsl.isAvailable() && config.nativeTlsEnabled() ? SslProvider.OPENSSL : SslProvider.JDK;

    SslContextBuilder context = SslContextBuilder.forClient().sslProvider(provider);

    if (config.trustManagerFactory() != null) {
      context.trustManager(config.trustManagerFactory());
    } else if (config.trustCertificates() != null && !config.trustCertificates().isEmpty()) {
      context.trustManager(config.trustCertificates().toArray(new X509Certificate[0]));
    }

    authenticator.applyTlsProperties(context);

    final SslHandler sslHandler = context.build().newHandler(
        allocator,
        peer.host(),
        peer.port()
    );

    SSLEngine sslEngine = sslHandler.engine();
    SSLParameters sslParameters = sslEngine.getSSLParameters();

    if (config.hostnameVerificationEnabled()) {
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    }

    sslEngine.setSSLParameters(sslParameters);

    return sslHandler;
  }

}
