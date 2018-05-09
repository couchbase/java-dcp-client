/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.util.Version;

import java.util.Collections;
import java.util.Map;

import static com.couchbase.client.dcp.message.HelloRequest.DATATYPE;
import static com.couchbase.client.dcp.message.HelloRequest.SNAPPY;

public enum CompressionMode {
    /**
     * Couchbase Server will never send compressed values.
     */
    DISABLED {
        @Override
        public CompressionMode effectiveMode(Version serverVersion) {
            // Can't fall back any further than this!
            return DISABLED;
        }

        @Override
        public Map<String, String> getDcpControls(Version serverVersion) {
            // Disabled is the default, so no need to set controls.
            return Collections.emptyMap();
        }

        @Override
        public short[] getHelloFeatures(Version serverVersion) {
            // Don't advertise support for compression.
            return EMPTY_SHORT_ARRAY;
        }
    },

    /**
     * Couchbase Server will always send compressed values, regardless
     * of how the values are stored on the server, unless the compressed
     * form is larger than the uncompressed form.
     * <p>
     * When connecting to a server older than version 4.5, this mode will be
     * downgraded to DISABLED.
     *
     * @since Couchbase Server 4.5 (Watson)
     */
    FORCED {
        @Override
        public CompressionMode effectiveMode(Version serverVersion) {
            return serverVersion.isAtLeast(WATSON) ? this : DISABLED;
        }

        @Override
        public Map<String, String> getDcpControls(Version serverVersion) {
            if (!serverVersion.isAtLeast(WATSON)) {
                // Programming error, should have called effectiveMode first to downgrade if compression is not supported.
                throw new IllegalArgumentException("Server version " + serverVersion + " does not support value compression");
            }

            // The name of this control changed in Vulcan, but the effect is the same.
            String controlName = serverVersion.isAtLeast(VULCAN)
                    ? "force_value_compression" : "enable_value_compression";

            return Collections.singletonMap(controlName, String.valueOf(true));
        }
    },

    /**
     * Default mode.
     * <p>
     * Couchbase Server may send compressed values at its discretion.
     * Values stored in compressed form will be sent compressed.
     * Values stored in uncompressed form will be sent uncompressed.
     * <p>
     * When connecting to a server older than version 5.5, this mode will be
     * downgraded to DISABLED.
     *
     * @since Couchbase Server 5.5 (Vulcan)
     */
    ENABLED {
        @Override
        public CompressionMode effectiveMode(Version serverVersion) {
            return serverVersion.isAtLeast(VULCAN) ? this : DISABLED;
        }

        @Override
        public Map<String, String> getDcpControls(Version serverVersion) {
            return Collections.emptyMap();
        }
    };

    private static final short[] EMPTY_SHORT_ARRAY = new short[0];

    /**
     * Watson only supports {@link #DISABLED} and {@link #FORCED}.
     * <p>
     * To enable {@link #FORCED} mode for Watson, set the `enable_value_compression`
     * control param to `true`.
     */
    private static final Version WATSON = new Version(4, 5, 0);

    /**
     * Vulcan supports {@link #DISABLED}, {@link #FORCED}, and {@link #ENABLED}.
     * <p>
     * If compression is to be used with Vulcan, the client must advertise the
     * DATATYPE and SNAPPY features when issuing the HELLO request.
     * <p>
     * To activate {@link #ENABLED} mode for Vulcan, all you need to do is HELLO with the
     * the aforementioned feature flags.
     * <p>
     * To enable {@link #FORCED} mode for Vulcan, HELLO with the aforementioned feature flags,
     * and then set the `force_value_compression` control param to `true`.
     */
    private static final Version VULCAN = new Version(5, 5, 0);

    /**
     * @return the mode specified by this enum value, or the fallback mode
     * as required by the actual server version.
     */
    public abstract CompressionMode effectiveMode(Version serverVersion);

    /**
     * @return the controls required to activate this compression mode.
     */
    public abstract Map<String, String> getDcpControls(Version serverVersion);

    /**
     * @return the HELLO features required to activate this compression mode.
     */
    public short[] getHelloFeatures(Version serverVersion) {
        // Vulcan won't send compressed values unless we HELLO with these features.
        // Watson doesn't care how we HELLO; doesn't hurt to send them anyway.
        // The DISABLED enum overrides this method to return no features,
        // deactivating compression for Vulcan.
        return new short[]{DATATYPE, SNAPPY};
    }
}
