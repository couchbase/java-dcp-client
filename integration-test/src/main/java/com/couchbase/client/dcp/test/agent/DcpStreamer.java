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

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.state.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.dcp.test.util.Poller.poll;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DcpStreamer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpStreamer.class);

  public static class Status {
    private final long mutations;
    private final long expirations;
    private final long deletions;

    public Status(long mutations, long expirations, long deletions) {
      this.mutations = mutations;
      this.expirations = expirations;
      this.deletions = deletions;
    }

    public long getMutations() {
      return mutations;
    }

    public long getExpirations() {
      return expirations;
    }

    public long getDeletions() {
      return deletions;
    }

    @Override
    public String toString() {
      return "Status{" +
          "mutations=" + mutations +
          ", expirations=" + expirations +
          ", deletions=" + deletions +
          '}';
    }
  }

  private final Client client;

  private final AtomicLong mutations = new AtomicLong();
  private final AtomicLong deletions = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();
  private final StreamTo streamTo;

  public DcpStreamer(final Client client, final List<Short> vbuckets,
                     final StreamFrom from, final StreamTo to) {
    this.client = requireNonNull(client);
    this.streamTo = requireNonNull(to);

    client.controlEventHandler((flowController, event) -> {
      if (DcpSnapshotMarkerRequest.is(event)) {
        flowController.ack(event);
      }
      event.release();
    });

    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        mutations.incrementAndGet();
      } else if (DcpDeletionMessage.is(event)) {
        deletions.incrementAndGet();
      } else if (DcpExpirationMessage.is(event)) {
        expirations.incrementAndGet();
      } else {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Received unexpected data event: {}", MessageUtil.humanize(event));
        }
      }

      flowController.ack(event);
      event.release();
    });

    client.connect().await(30, TimeUnit.SECONDS);
    try {
      client.initializeState(from, to).await();
      client.startStreaming(vbuckets.toArray(new Short[0])).await();
    } catch (Throwable t) {
      stop();
      throw t;
    }
  }

  public Status awaitStreamEnd(long timeout, TimeUnit unit) throws TimeoutException {
    if (this.streamTo == StreamTo.INFINITY) {
      throw new IllegalStateException("Streaming to infinity; can't wait for that!");
    }

    poll().atInterval(100, MILLISECONDS)
        .withTimeout(timeout, unit)
        .until(() -> client.sessionState().isAtEnd());

    return status();
  }

  public Status awaitMutationCount(int mutationCount, long timeout, TimeUnit unit) {
    poll().atInterval(100, MILLISECONDS)
        .withTimeout(timeout, unit)
        .untilTimeExpiresOr(() -> mutations.get() >= mutationCount);

    return status();
  }

  public Status status() {
    return new Status(mutations.get(), expirations.get(), deletions.get());
  }

  SessionState getSessionState() {
    return client.sessionState();
  }

  void stop() {
    client.disconnect().await();
  }
}
