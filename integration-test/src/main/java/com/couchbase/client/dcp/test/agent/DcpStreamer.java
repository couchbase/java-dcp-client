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
import com.couchbase.client.dcp.highlevel.CollectionCreated;
import com.couchbase.client.dcp.highlevel.CollectionDropped;
import com.couchbase.client.dcp.highlevel.CollectionFlushed;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.FlowControlMode;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.Rollback;
import com.couchbase.client.dcp.highlevel.ScopeCreated;
import com.couchbase.client.dcp.highlevel.ScopeDropped;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.state.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static com.couchbase.client.dcp.test.util.Poller.poll;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DcpStreamer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpStreamer.class);

  /**
   * {@link Status} states
   */
  public enum State {
    MUTATIONS,
    EXPIRATIONS,
    DELETIONS,
    SCOPE_CREATIONS,
    SCOPE_DROPS,
    COLLECTION_CREATIONS,
    COLLECTION_DROPS,
    COLLECTION_FLUSHES,
    STREAM_FAILURES,
    ROLLBACKS,
  }

  public static class Status {
    private final long mutations;
    private final long expirations;
    private final long deletions;
    private final long scopeCreations;
    private final long scopeDrops;
    private final long collectionCreations;
    private final long collectionDrops;
    private final long collectionFlushes;
    private final long streamFailures;
    private final long rollbacks;


    public Status(long mutations, long expirations, long deletions, long scopeCreations, long scopeDrops, long collectionCreations, long collectionDrops, long collectionFlushes, long streamFailures, long rollbacks) {
      this.mutations = mutations;
      this.expirations = expirations;
      this.deletions = deletions;
      this.scopeCreations = scopeCreations;
      this.scopeDrops = scopeDrops;
      this.collectionCreations = collectionCreations;
      this.collectionDrops = collectionDrops;
      this.collectionFlushes = collectionFlushes;
      this.streamFailures = streamFailures;
      this.rollbacks = rollbacks;
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

    public long getScopeCreations() {
      return scopeCreations;
    }

    public long getScopeDrops() {
      return scopeDrops;
    }

    public long getCollectionCreations() {
      return collectionCreations;
    }

    public long getCollectionDrops() {
      return collectionDrops;
    }

    public long getCollectionFlushes() {
      return collectionFlushes;
    }

    public long getStreamFailures() {
      return streamFailures;
    }

    public long getRollbacks() {
      return rollbacks;
    }

    public long getStateCount(State state) {
      switch (state) {
        case DELETIONS:
          return getDeletions();
        case MUTATIONS:
          return getMutations();
        case EXPIRATIONS:
          return getExpirations();
        case SCOPE_CREATIONS:
          return getScopeCreations();
        case SCOPE_DROPS:
          return getScopeDrops();
        case COLLECTION_CREATIONS:
          return getCollectionCreations();
        case COLLECTION_DROPS:
          return getCollectionDrops();
        case COLLECTION_FLUSHES:
          return getCollectionFlushes();
        case STREAM_FAILURES:
          return getStreamFailures();
        case ROLLBACKS:
          return getRollbacks();
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public String toString() {
      return "Status{" +
          "mutations=" + mutations +
          ", expirations=" + expirations +
          ", deletions=" + deletions +
          ", scopeCreations=" + scopeCreations +
          ", scopeDrops=" + scopeDrops +
          ", collectionCreations=" + collectionCreations +
          ", collectionDrops=" + collectionDrops +
          ", collectionFlushes=" + collectionFlushes +
          ", streamFailures=" + streamFailures +
          ", rollbacks=" + rollbacks +
          '}';
    }
  }

  private final Client client;

  private final LongAdder mutations = new LongAdder();
  private final LongAdder deletions = new LongAdder();
  private final LongAdder expirations = new LongAdder();
  private final LongAdder scopeCreations = new LongAdder();
  private final LongAdder scopeDrops = new LongAdder();
  private final LongAdder collectionCreations = new LongAdder();
  private final LongAdder collectionDrops = new LongAdder();
  private final LongAdder collectionFlushes = new LongAdder();
  private final LongAdder streamFailures = new LongAdder();
  private final LongAdder rollbacks = new LongAdder();

  private final StreamTo streamTo;

  public DcpStreamer(final Client client, final List<Integer> vbuckets,
                     final StreamFrom from, final StreamTo to) {
    this.client = requireNonNull(client);
    this.streamTo = requireNonNull(to);

    client.listener(new DatabaseChangeListener() {
      @Override
      public void onRollback(Rollback rollback) {
        rollbacks.increment();
        rollback.resume();
      }

      @Override
      public void onFailure(StreamFailure streamFailure) {
        streamFailures.increment();
        LOGGER.error("stream failure", streamFailure.getCause());
      }

      @Override
      public void onMutation(Mutation mutation) {
        mutations.increment();
        mutation.flowControlAck();
      }

      @Override
      public void onDeletion(Deletion deletion) {
        (deletion.isDueToExpiration() ? expirations : deletions).increment();
        deletion.flowControlAck();
      }

      @Override
      public void onScopeCreated(ScopeCreated scopeCreated) {
        scopeCreations.increment();
      }

      @Override
      public void onScopeDropped(ScopeDropped scopeDropped) {
        scopeDrops.increment();
      }

      @Override
      public void onCollectionCreated(CollectionCreated collectionCreated) {
        collectionCreations.increment();
      }

      @Override
      public void onCollectionDropped(CollectionDropped collectionDropped) {
        collectionDrops.increment();
      }

      @Override
      public void onCollectionFlushed(CollectionFlushed collectionFlushed) {
        collectionFlushes.increment();
      }
    }, FlowControlMode.AUTOMATIC);

    client.connect().block(Duration.ofSeconds(30));
    try {
      client.initializeState(from, to).block();
      client.startStreaming(vbuckets).block();
    } catch (Throwable t) {
      stop();
      throw t;
    }
  }

  public Status awaitStreamEnd(long timeout, TimeUnit unit) {
    if (this.streamTo == StreamTo.INFINITY) {
      throw new IllegalStateException("Streaming to infinity; can't wait for that!");
    }

    poll().atInterval(100, MILLISECONDS)
        .withTimeout(timeout, unit)
        .until(() -> client.sessionState().isAtEnd());

    return status();
  }

  public Status awaitStateCount(State state, int stateCount, long timeout, TimeUnit unit) {
    poll().atInterval(100, MILLISECONDS)
        .withTimeout(timeout, unit)
        .untilTimeExpiresOr(() -> status().getStateCount(state) >= stateCount);
    return status();
  }

  public Status status() {
    return new Status(
        mutations.sum(),
        expirations.sum(),
        deletions.sum(),
        scopeCreations.sum(),
        scopeDrops.sum(),
        collectionCreations.sum(),
        collectionDrops.sum(),
        collectionFlushes.sum(),
        streamFailures.sum(),
        rollbacks.sum()
    );
  }

  SessionState getSessionState() {
    return client.sessionState();
  }

  void stop() {
    client.disconnect().block();
  }
}
