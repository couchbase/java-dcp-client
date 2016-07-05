package com.couchbase.client.dcp.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Container for all configurable DCP control params.
 */
public class DcpControl implements Iterable<Map.Entry<String, String>> {

    private Map<String, String> values;

    public DcpControl() {
        this.values = new HashMap<String, String>();
    }

    public DcpControl put(Names name, String value) {
        values.put(name.value(), value);
        return this;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return values.entrySet().iterator();
    }

    public static enum Names {
        NOOP_ENABLED("enable_noop"),
        NOOP_INTERVAL("set_noop_interval"),
        CONNECTION_BUFFER_SIZE("connection_buffer_size");

        private String value;

        Names(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}
