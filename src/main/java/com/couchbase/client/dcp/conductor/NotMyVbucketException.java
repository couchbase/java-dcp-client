package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.CouchbaseException;

/**
 * Created by daschl on 01/09/16.
 */
public class NotMyVbucketException extends CouchbaseException {

    public NotMyVbucketException() {
    }

    public NotMyVbucketException(String message) {
        super(message);
    }

    public NotMyVbucketException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotMyVbucketException(Throwable cause) {
        super(cause);
    }
}
