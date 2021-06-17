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

package com.couchbase.client.dcp.highlevel.internal;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.events.DcpFailureEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.FailoverLog;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.Rollback;
import com.couchbase.client.dcp.highlevel.SeqnoAdvanced;
import com.couchbase.client.dcp.highlevel.SnapshotDetails;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamEnd;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSeqnoAdvancedRequest;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.DcpSystemEventRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.user;
import static com.couchbase.client.dcp.message.MessageUtil.getOpcode;
import static java.util.Objects.requireNonNull;

/**
 * Bridges the gap between the various low-level callback handlers and the
 * high-level {@link com.couchbase.client.dcp.highlevel.DatabaseChangeListener}.
 */
public class EventHandlerAdapter implements ControlEventHandler, SystemEventHandler {
  private static final Logger log = LoggerFactory.getLogger(EventHandlerAdapter.class);

  private static final int MAX_PARTITIONS = 1024;

  private final Client dcpClient;
  private final EventDispatcher dispatcher;

  // Duplicate some of the partition state bookkeeping so it's easier to reason about the event handler state
  private final AtomicLongArray vbucketToUuid = new AtomicLongArray(MAX_PARTITIONS);
  private final AtomicReferenceArray<SnapshotMarker> vbucketToCurrentSnapshot = new AtomicReferenceArray<>(MAX_PARTITIONS);

  private EventHandlerAdapter(Client dcpClient, EventDispatcher dispatcher) {
    this.dcpClient = requireNonNull(dcpClient);
    this.dispatcher = requireNonNull(dispatcher);

    dcpClient.controlEventHandler(this);
    dcpClient.systemEventHandler(this);
    dcpClient.dataEventHandler(dataEventHandler);
  }

  public static EventHandlerAdapter register(Client dcpClient, EventDispatcher dispatcher) {
    return new EventHandlerAdapter(dcpClient, dispatcher);
  }

  private void dispatch(DatabaseChangeEvent event) {
    dispatcher.dispatch(event);
  }

  private void dispatchOrLogError(StreamFailure event) {
    try {
      dispatch(event);
    } catch (Throwable t) {
      // There's not much to be done at this point, since the error can't even be reported
      // to the user. Swallow it so it doesn't propagate to the Netty event loop.
      log.error("Error occurred during stream failure event dispatch.", t);
    }
  }

  @Override
  public void onEvent(CouchbaseEvent event) {
    try {
      if (event instanceof StreamEndEvent) {
        StreamEndEvent streamEnd = (StreamEndEvent) event;
        dispatch(new StreamEnd(streamEnd.partition(), streamEnd.reason()));

      } else if (event instanceof DcpFailureEvent) {
        DcpFailureEvent fail = (DcpFailureEvent) event;
        dispatch(new StreamFailure(fail.partition().orElse(-1), fail.error()));

      } else {
        log.debug("Ignoring unrecognized system event: {}", event.toMap());
      }
    } catch (Throwable t) {
      log.error("Failed to dispatch system event", t);
      dispatchOrLogError(new StreamFailure(-1, t));
    }
  }

  @Override
  public void onEvent(ChannelFlowController flowController, ByteBuf event) {
    try {
      flowController.ack(event); // immediately ACK snapshot markers, SEQNO_ADVANCED, etc.

      final byte opcode = event.getByte(1);
      switch (opcode) {
        case MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE: {
          final SnapshotMarker marker = new SnapshotMarker(
              DcpSnapshotMarkerRequest.startSeqno(event),
              DcpSnapshotMarkerRequest.endSeqno(event));
          final int flags = DcpSnapshotMarkerRequest.flags(event);
          final int vbucket = MessageUtil.getVbucket(event);
          vbucketToCurrentSnapshot.set(vbucket, marker);
          dispatch(new SnapshotDetails(vbucket, flags, marker));
          return;
        }

        case MessageUtil.INTERNAL_ROLLBACK_OPCODE: {
          final int vbucket = RollbackMessage.vbucket(event); // note the different accessor!
          final Consumer<Throwable> defaultErrorHandler = t -> dispatch(new StreamFailure(vbucket, t));
          dispatch(new Rollback(dcpClient, vbucket, RollbackMessage.seqno(event), defaultErrorHandler));
          return;
        }

        case MessageUtil.DCP_FAILOVER_LOG_OPCODE: {
          final int vbucket = DcpFailoverLogResponse.vbucket(event);
          final List<FailoverLogEntry> failoverLog = DcpFailoverLogResponse.entries(event);
          vbucketToUuid.set(vbucket, failoverLog.get(0).getUuid());
          dispatch(new FailoverLog(vbucket, failoverLog));
          return;
        }

        case MessageUtil.DCP_SEQNO_ADVANCED_OPCODE: {
          final int vbucket = MessageUtil.getVbucket(event);
          final long seqno = DcpSeqnoAdvancedRequest.getSeqno(event);
          vbucketToCurrentSnapshot.set(vbucket, new SnapshotMarker(seqno, seqno));

          log.debug("vbucket {} seqno advanced to {}", vbucket, seqno);
          dispatch(new SeqnoAdvanced(vbucket, newOffset(vbucket, seqno)));
          return;
        }

        case MessageUtil.DCP_SYSTEM_EVENT_OPCODE: {
          // Partition state updates are handled by the low-level control event handler.
          // All we need to do here is dispatch the event to the high-level listener.
          final DcpSystemEvent sysEvent = DcpSystemEvent.parse(event);
          if (sysEvent instanceof DatabaseChangeEvent) {
            log.debug("Received system event: {}", sysEvent);
            dispatch((DatabaseChangeEvent) sysEvent);
          } else {
            log.warn("Received unrecognized system event:\n{}", MessageUtil.humanize(event));
          }

          final int vbucket = MessageUtil.getVbucket(event);
          final long seqno = DcpSystemEventRequest.getSeqno(event);
          log.debug("vbucket {} seqno advanced to {} due to system event", vbucket, seqno);
          dispatch(new SeqnoAdvanced(vbucket, newOffset(vbucket, seqno)));
          return;
        }

        default:
          log.warn("Unexpected control event type: {}", MessageUtil.getShortOpcodeName(getOpcode(event)));
      }

    } catch (Throwable t) {
      log.error("Failed to dispatch control event", t);
      dispatchOrLogError(new StreamFailure(-1, t));

    } finally {
      event.release();
    }
  }

  private CollectionsManifest getCurrentManifest(int vbucket) {
    return dcpClient.sessionState()
        .get(vbucket)
        .getCollectionsManifest();
  }

  private CollectionIdAndKey extractKey(int vbucket, ByteBuf event) {
    return dcpClient.sessionState()
        .get(vbucket)
        .getKeyExtractor()
        .getCollectionIdAndKey(event);
  }

  private StreamOffset newOffset(ByteBuf dataEvent, int vbucket) {
    long seqno = DcpMutationMessage.bySeqno(dataEvent); // works for deletion/expiration events too
    return newOffset(vbucket, seqno);
  }

  private StreamOffset newOffset(int vbucket, long seqno) {
    final long vbuuid = vbucketToUuid.get(vbucket);
    final SnapshotMarker snapshot = vbucketToCurrentSnapshot.get(vbucket);
    final long collectionsManifestUid = dcpClient.sessionState().get(vbucket).getCollectionsManifestUid();
    try {
      return new StreamOffset(vbuuid, seqno, snapshot, collectionsManifestUid);

    } catch (IllegalArgumentException e) {
      log.error("Invalid stream offset detected for partition {} with UUID {}: {}\nClient's view of session state: {}",
          vbucket, vbuuid, e.getMessage(), dcpClient.sessionState().get(vbucket));
      throw e;
    }
  }

  private final DataEventHandler dataEventHandler = (flowController, event) -> {
    int vbucket = -1;

    try {
      final FlowControlReceipt receipt = FlowControlReceipt.forMessage(flowController, event);
      vbucket = MessageUtil.getVbucket(event);
      final CollectionsManifest manifest = getCurrentManifest(vbucket);

      final byte opcode = event.getByte(1);
      switch (opcode) {
        case MessageUtil.DCP_MUTATION_OPCODE: {
          final CollectionIdAndKey collectionIdAndKey = extractKey(vbucket, event);
          final CollectionsManifest.CollectionInfo collectionInfo = manifest.getCollection(collectionIdAndKey.collectionId());
          if (collectionInfo == null) {
            log.warn("Unrecognized collection ID {} for key {}; assuming collection was deleted, and skipping",
                collectionIdAndKey.collectionId(), user(collectionIdAndKey.key()));

            dispatch(new SeqnoAdvanced(vbucket, newOffset(event, vbucket)));
            return;
          }

          dispatch(new Mutation(event, collectionInfo, collectionIdAndKey.key(), receipt, newOffset(event, vbucket)));
          return;
        }

        case MessageUtil.DCP_DELETION_OPCODE: {
          final CollectionIdAndKey collectionIdAndKey = extractKey(vbucket, event);
          final CollectionsManifest.CollectionInfo collectionInfo = manifest.getCollection(collectionIdAndKey.collectionId());

          dispatch(new Deletion(event, collectionInfo, collectionIdAndKey.key(), receipt, newOffset(event, vbucket), false));
          return;
        }

        case MessageUtil.DCP_EXPIRATION_OPCODE: {
          final CollectionIdAndKey collectionIdAndKey = extractKey(vbucket, event);
          final CollectionsManifest.CollectionInfo collectionInfo = manifest.getCollection(collectionIdAndKey.collectionId());

          dispatch(new Deletion(event, collectionInfo, collectionIdAndKey.key(), receipt, newOffset(event, vbucket), true));
          return;
        }

        default:
          receipt.acknowledge();
          log.warn("Unexpected data event type: {}", MessageUtil.getShortOpcodeName(event));
      }
    } catch (Throwable t) {
      log.error("Failed to dispatch data event", t);
      dispatchOrLogError(new StreamFailure(vbucket, t));

    } finally {
      event.release();
    }
  };
}
