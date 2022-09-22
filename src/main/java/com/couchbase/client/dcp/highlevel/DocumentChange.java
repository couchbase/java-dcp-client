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

import com.couchbase.client.dcp.core.logging.RedactableArgument;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import com.couchbase.client.dcp.highlevel.internal.FlowControlReceipt;
import com.couchbase.client.dcp.highlevel.internal.FlowControllable;
import com.couchbase.client.dcp.message.ContentAndXattrs;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class DocumentChange implements DatabaseChangeEvent, FlowControllable {
  private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

  private static final AtomicLong nextTracingToken = new AtomicLong();

  private final long tracingToken = nextTracingToken.getAndIncrement();
  private final int vbucket;
  private final StreamOffset offset;
  private final ContentAndXattrs contentAndXattrs;
  private final boolean mutation;
  private final long revision;
  private final long cas;
  private final CollectionsManifest.CollectionInfo collectionInfo;
  private final String key;

  private final FlowControlReceipt receipt;

  public DocumentChange(ByteBuf byteBuf, CollectionsManifest.CollectionInfo collectionInfo, String key, FlowControlReceipt receipt, StreamOffset offset) {
    this.vbucket = MessageUtil.getVbucket(byteBuf);
    this.mutation = DcpMutationMessage.is(byteBuf);
    this.collectionInfo = requireNonNull(collectionInfo);
    this.key = requireNonNull(key);

    this.revision = DcpMutationMessage.revisionSeqno(byteBuf); // same method works for deletion and expiration, too

    this.offset = requireNonNull(offset);
    this.receipt = requireNonNull(receipt);
    this.contentAndXattrs = MessageUtil.getContentAndXattrs(byteBuf);
    this.cas = MessageUtil.getCas(byteBuf);
  }

  /**
   * Returns an opaque tracing token that uniquely identifies this change
   * within the current run of the JVM.
   * <p>
   * Useful for correlating lifecycle log messages.
   */
  public long getTracingToken() {
    return tracingToken;
  }

  /**
   * Returns the document's extended attributes (XATTRS) as a map.
   * <p>
   * The keys are the attribute names, and the values are the corresponding
   * attribute values encoded as JSON.
   * <p>
   * If the DCP client was not initialized to request extended attributes,
   * this method always returns an empty map.
   *
   * @see com.couchbase.client.dcp.Client.Builder#xattrs(boolean)
   */
  public Map<String, String> getXattrs() {
    return contentAndXattrs.xattrs();
  }

  public byte[] getContent() {
    return contentAndXattrs.content();
  }

  public int getVbucket() {
    return vbucket;
  }

  public StreamOffset getOffset() {
    return offset;
  }

  public String getKey() {
    return key;
  }

  /**
   * Returns the document key prefixed by the names of the containing scope and collection.
   */
  public String getQualifiedKey() {
    return collectionInfo.scope().name() + "." + collectionInfo.name() + "." + key;
  }

  public CollectionsManifest.CollectionInfo getCollection() {
    return collectionInfo;
  }

  public boolean isMutation() {
    return mutation;
  }

  public long getRevision() {
    return revision;
  }

  public long getCas() {
    return cas;
  }

  /**
   * Returns the time the change occurred.
   * <p>
   * <b>CAVEAT:</b> In order for the timestamp in the CAS to be reliable,
   * the bucket must have been created by Couchbase Server 4.6 or later,
   * and the document change must have been performed by Couchbase Server 7.0 or later.
   * Even then, it's possible for a set_with_meta operation to assign an
   * arbitrary CAS value (and therefore timestamp) to a document.
   */
  public Instant getTimestamp() {
    // NOTE: The structure of a CAS value is Couchbase internal API.
    // User-level code should treat CAS values as opaque.

    // The low 16 bits contain a logical clock that isn't part of the timestamp.
    long epochNano = cas & 0xffffffffffff0000L;

    // Interpret as unsigned so this overflows in the year 2554 instead of 2262.
    long epochSecond = Long.divideUnsigned(epochNano, NANOS_PER_SECOND);
    long nanoAdjustment = Long.remainderUnsigned(epochNano, NANOS_PER_SECOND);
    return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
  }

  /**
   * Removes the backpressure generated by this event, allowing the server
   * to send more data.
   * <p>
   * If flow control is enabled on the client, then non-blocking listeners
   * and listeners using {@link FlowControlMode#MANUAL} <b>MUST</b> call
   * this method when the application is ready to receive more data
   * (usually when the app has finished processing the event),
   * otherwise the server will stop sending events.
   * <p>
   * This method is idempotent; if it is called more than once, any
   * calls after the first are ignored.
   */
  @Override
  public void flowControlAck() {
    receipt.acknowledge();
  }

  @Override
  public String toString() {
    final String type = isMutation() ? "MUT" : "DEL";
    return type + ":" + getVbucket() + "/" + getOffset() + "=" + RedactableArgument.user(getQualifiedKey());
  }
}
