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

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class Rollback implements DatabaseChangeEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(Rollback.class);

  private final Client client;
  private final int vbucket;
  private final long seqno;
  private final Consumer<Throwable> errorHandler;
  private final StreamOffset failedStartOffset;

  public Rollback(Client client, int vbucket, long seqno, Consumer<Throwable> errorHandler) {
    this.client = requireNonNull(client);
    this.vbucket = vbucket;
    this.seqno = seqno;
    this.errorHandler = requireNonNull(errorHandler);

    // A zero offset can never cause a rollback -- just need this to be non-null.
    // Decided not to expose the optionality to the user, since a null value indicates a DCP client bug.
    this.failedStartOffset = Optional.ofNullable(client.sessionState().get(vbucket).getMostRecentOpenStreamOffset())
        .orElse(StreamOffset.ZERO);
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
   * Returns the requested start offset that lead to this rollback.
   * <p>
   * Useful only for diagnostic purposes.
   */
  public StreamOffset getFailedStartOffset() {
    return failedStartOffset;
  }

  /**
   * Reopens the stream starting from the rollback point, without any snapshot information.
   */
  public void resume() {
    client.rollbackAndRestartStream(vbucket, seqno)
        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(10))
            .maxBackoff(Duration.ofSeconds(5))
            .doAfterRetry(retrySignal -> LOGGER.info("Retrying rollbackAndRestartStream for vbucket {}", vbucket))
        )
        .onErrorResume(t -> {
          errorHandler.accept(t);
          return Mono.empty();
        })
        .doOnSuccess(ignore -> LOGGER.info("Rollback for partition {} complete!", vbucket))
        .subscribe();
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onRollback(this);
  }
}
