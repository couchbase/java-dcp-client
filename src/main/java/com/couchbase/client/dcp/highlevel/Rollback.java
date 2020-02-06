/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.highlevel;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.couchbase.client.dcp.util.retry.RetryBuilder.any;
import static java.util.Objects.requireNonNull;

public class Rollback implements DatabaseChangeEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(Rollback.class);

  private final Client client;
  private final int vbucket;
  private final long seqno;
  private final Consumer<Throwable> errorHandler;

  public Rollback(Client client, int vbucket, long seqno, Consumer<Throwable> errorHandler) {
    this.client = requireNonNull(client);
    this.vbucket = vbucket;
    this.seqno = seqno;
    this.errorHandler = requireNonNull(errorHandler);
  }

  public int getVbucket() {
    return vbucket;
  }

  /**
   * <b>NOTE:</b> Sequence numbers are unsigned, and must be compared using
   * {@link Long#compareUnsigned(long, long)}
   */
  public long getSeqno() {
    return seqno;
  }

  /**
   * Reopens the stream starting from the rollback point, without any snapshot information.
   */
  public void resume() {
    client.rollbackAndRestartStream((short) vbucket, seqno)
        .retryWhen(any()
            .max(Integer.MAX_VALUE)
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, TimeUnit.SECONDS.toMillis(5)))
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.info("Retrying rollbackAndRestartStream for vbucket {}", vbucket))
            .build())
        .subscribe(
            () -> LOGGER.info("Rollback for partition {} complete!", vbucket),
            errorHandler::accept);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onRollback(this);
  }
}
