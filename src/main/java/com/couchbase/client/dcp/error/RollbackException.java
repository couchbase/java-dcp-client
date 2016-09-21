package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

/**
 * Indicates a rollback happened, used as an internal messaging event.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class RollbackException extends CouchbaseException {

    public RollbackException() {
    }

    public RollbackException(String message) {
        super(message);
    }

    public RollbackException(String message, Throwable cause) {
        super(message, cause);
    }

    public RollbackException(Throwable cause) {
        super(cause);
    }
}
