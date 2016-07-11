package com.couchbase.client.dcp.conductor;

import java.net.InetAddress;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel {

    private final InetAddress inetAddress;

    public DcpChannel(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
    }

    public void connect() {

    }

    public void disconnect() {

    }

    @Override
    public boolean equals(Object o) {
        return inetAddress.equals(o);
    }

    @Override
    public int hashCode() {
        return inetAddress.hashCode();
    }
}
