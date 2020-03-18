package com.couchbase.client.dcp.error;

import com.couchbase.client.dcp.core.CouchbaseException;

/**
 * Indicates a rollback happened, used as an internal messaging event.
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
