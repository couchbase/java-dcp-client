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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static com.couchbase.client.dcp.buffer.StreamEventBuffer.BufferedEvent.Type.CONTROL;
import static com.couchbase.client.dcp.buffer.StreamEventBuffer.BufferedEvent.Type.DATA;
import static com.couchbase.client.dcp.buffer.StreamEventBuffer.BufferedEvent.Type.STREAM_END_OK;
import static java.util.Collections.unmodifiableList;

/**
 * When rollback mitigation / persistence polling is enabled, the stream event buffer
 * intercepts stream events and stores them until being notified that persistence is observed.
 * Then it forwards the events to the handlers set by the user.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class StreamEventBuffer implements DataEventHandler, ControlEventHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamEventBuffer.class);

  static class BufferedEvent {
    enum Type {DATA, CONTROL, STREAM_END_OK}

    private final long seqno;
    private final ByteBuf event;
    private final ChannelFlowController flowController;
    private final Type type;

    BufferedEvent(final long seqno, final ByteBuf event, final ChannelFlowController flowController, final Type type) {
      this.seqno = seqno;
      this.event = event;
      this.flowController = flowController;
      this.type = type;
    }

    static BufferedEvent streamEnd(final long seqno) {
      return new BufferedEvent(seqno, null, null, STREAM_END_OK);

    }

    void discard() {
      try {
        flowController.ack(event);
      } catch (Throwable t) {
        LOGGER.debug("Failed to ack buffered event; channel already closed?", t);
      } finally {
        event.release();
      }
    }
  }

  private static final int MAX_PARTITIONS = 1024;

  private final EventBus eventBus;
  private volatile DataEventHandler dataEventHandler;
  private volatile ControlEventHandler controlEventHandler;
  private final List<Deque<BufferedEvent>> partitionQueues; // one queue for each partition

  public StreamEventBuffer(EventBus eventBus) {
    this.eventBus = eventBus;

    final List<Deque<BufferedEvent>> partitionQueues = new ArrayList<>(MAX_PARTITIONS);
    for (int i = 0; i < MAX_PARTITIONS; i++) {
      partitionQueues.add(new ArrayDeque<>());
    }
    this.partitionQueues = unmodifiableList(partitionQueues);
  }

  public void setDataEventHandler(final DataEventHandler dataEventHandler) {
    this.dataEventHandler = dataEventHandler;
  }

  public void setControlEventHandler(final ControlEventHandler controlEventHandler) {
    this.controlEventHandler = controlEventHandler;
  }

  @Override
  public void onEvent(final ChannelFlowController flowController, final ByteBuf event) {
    if (DcpMutationMessage.is(event) || DcpDeletionMessage.is(event) || DcpExpirationMessage.is(event)) {
      // Mutation, deletion, and expiration messages all have seqno in same location.
      final long seqno = DcpMutationMessage.bySeqno(event);
      final short vbucket = MessageUtil.getVbucket(event);
      enqueue(vbucket, new BufferedEvent(seqno, event, flowController, DATA));

    } else if (DcpSnapshotMarkerRequest.is(event)) {
      final long seqno = DcpSnapshotMarkerRequest.startSeqno(event);
      final short vbucket = MessageUtil.getVbucket(event);
      enqueue(vbucket, new BufferedEvent(seqno, event, flowController, CONTROL));

    } else if (RollbackMessage.is(event)) {
      rollback(RollbackMessage.vbucket(event), RollbackMessage.seqno(event));
      controlEventHandler.onEvent(flowController, event);

    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Propagating unhandled control event: {}", MessageUtil.humanize(event));
      }
      controlEventHandler.onEvent(flowController, event);
    }
  }

  public void onStreamEnd(StreamEndEvent event) {
    final Deque<BufferedEvent> queue = partitionQueues.get(event.partition());
    synchronized (queue) {
      if (event.reason() != StreamEndReason.OK) {
        // Conductor.maybeMovePartition() will restart the stream from what it thinks is the current state.
        // Buffered events are not reflected in the session state, so clear the buffer.
        clear(event.partition());

        eventBus.publish(event);
        return;
      }

      // The stream ended because server has sent everything up to the requested end seqno.
      // Hide this fact from the client until persistence is observed for all buffered events.

      if (queue.isEmpty()) {
        // No buffered events. Propagate immediately!
        eventBus.publish(event);
        return;
      }

      // Stream end events don't have sequence numbers. Fake it by assigning the same
      // seqno as the last event in the queue, so it will be propagated at the same time.
      long fakeSeqno = queue.peekLast().seqno;
      queue.add(BufferedEvent.streamEnd(fakeSeqno));
    }
  }

  private void enqueue(short vbucket, BufferedEvent event) {
    final Queue<BufferedEvent> queue = partitionQueues.get(vbucket);
    synchronized (queue) {
      queue.add(event);
    }
  }

  /**
   * Discard all buffered events in the given vbucket.
   */
  public void clear(final short vbucket) {
    final Queue<BufferedEvent> queue = partitionQueues.get(vbucket);
    synchronized (queue) {
      LOGGER.debug("Clearing stream event buffer for partition {}", vbucket);
      for (BufferedEvent bufferedEvent : queue) {
        bufferedEvent.discard();
      }
      queue.clear();
    }
  }

  /**
   * Discard any buffered events in the given vBucket with sequence numbers
   * higher than the given sequence number.
   */
  private void rollback(final short vbucket, final long toSeqno) {
    final Queue<BufferedEvent> queue = partitionQueues.get(vbucket);

    synchronized (queue) {
      for (Iterator<BufferedEvent> i = queue.iterator(); i.hasNext(); ) {
        final BufferedEvent event = i.next();
        final boolean eventSeqnoIsGreaterThanRollbackSeqno = Long.compareUnsigned(event.seqno, toSeqno) > 0;
        if (eventSeqnoIsGreaterThanRollbackSeqno) {
          LOGGER.trace("Dropping event with seqno {} from stream buffer for partition {}", event.seqno, vbucket);
          event.discard();
          i.remove();
        }
      }
    }
  }

  boolean hasBufferedEvents(final short vbucket) {
    final Queue<BufferedEvent> queue = partitionQueues.get(vbucket);
    synchronized (queue) {
      return !queue.isEmpty();
    }
  }

  /**
   * Send to the wrapped handler all events with sequence numbers <= the given sequence number.
   */
  void onSeqnoPersisted(final short vbucket, final long seqno) {
    final Queue<BufferedEvent> queue = partitionQueues.get(vbucket);

    synchronized (queue) {
      // while the head of the queue has a seqno <= the given seqno
      while (!queue.isEmpty() && Long.compareUnsigned(queue.peek().seqno, seqno) < 1) {
        final BufferedEvent event = queue.poll();

        try {
          switch (event.type) {
            case DATA: // mutation, deletion, expiration
              dataEventHandler.onEvent(event.flowController, event.event);
              break;

            case CONTROL: // snapshot
              controlEventHandler.onEvent(event.flowController, event.event);
              break;

            case STREAM_END_OK:
              eventBus.publish(new StreamEndEvent(vbucket, StreamEndReason.OK));
              break;

            default:
              throw new RuntimeException("Unexpected event type: " + event.type);
          }

        } catch (Throwable t) {
          LOGGER.error("Event handler threw exception", t);
        }
      }
    }
  }
}
