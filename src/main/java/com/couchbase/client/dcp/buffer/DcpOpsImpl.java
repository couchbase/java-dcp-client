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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.transport.netty.DcpResponse;
import com.couchbase.client.dcp.transport.netty.DcpResponseListener;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.dcp.buffer.DcpOpsImpl.DcpRequestBuilder.request;
import static java.util.Objects.requireNonNull;

public class DcpOpsImpl implements DcpOps {
  private final DcpRequestDispatcher dispatcher;

  public DcpOpsImpl(final DcpRequestDispatcher dispatcher) {
    this.dispatcher = requireNonNull(dispatcher);
  }

  @Override
  public Mono<ObserveSeqnoResponse> observeSeqno(final int partition, final long vbuuid) {
    return doRequest(
        () -> request(MessageUtil.OBSERVE_SEQNO_OPCODE)
            .vbucket(partition)
            .content(Unpooled.buffer(8).writeLong(vbuuid)),
        ObserveSeqnoResponse::new);
  }

  @Override
  public Mono<FailoverLogResponse> getFailoverLog(final int partition) {
    return doRequest(
        () -> request(MessageUtil.DCP_FAILOVER_LOG_OPCODE)
            .vbucket(partition),
        FailoverLogResponse::new);
  }

  private <R> Mono<R> doRequest(final Supplier<DcpRequestBuilder> requestBuilder, final Function<ByteBuf, R> resultExtractor) {
    return Mono.create(sink -> {
      try {
        final ByteBuf request = requestBuilder.get().build();

        dispatcher.sendRequest(request).addListener(new DcpResponseListener() {
          @Override
          public void operationComplete(Future<DcpResponse> future) {
            if (!future.isSuccess()) {
              sink.error(future.cause());
              return;
            }

            final ByteBuf buf = future.getNow().buffer();
            try {
              final ResponseStatus status = MessageUtil.getResponseStatus(buf);
              if (!status.isSuccess()) {
                throw new BadResponseStatusException(status);
              }

              final R result = resultExtractor.apply(buf);
              sink.success(result);

            } catch (Throwable t) {
              sink.error(t);

            } finally {
              buf.release();
            }
          }
        });

      } catch (Throwable t) {
        sink.error(t);
      }
    });
  }

  /**
   * NOT REUSABLE
   */
  static class DcpRequestBuilder {
    private final byte opcode;
    private int vbucket;
    private ByteBuf content;
    private boolean used;

    private DcpRequestBuilder(byte opcode) {
      this.opcode = opcode;
    }

    static DcpRequestBuilder request(byte opcode) {
      return new DcpRequestBuilder(opcode);
    }

    DcpRequestBuilder vbucket(int vbucket) {
      this.vbucket = vbucket;
      return this;
    }

    DcpRequestBuilder content(ByteBuf content) {
      this.content = content;
      return this;
    }

    ByteBuf build() {
      if (used) {
        throw new IllegalStateException("Not reusable");
      }
      try {
        ByteBuf buf = Unpooled.buffer();
        MessageUtil.initRequest(opcode, buf);
        MessageUtil.setVbucket(vbucket, buf);
        if (content != null) {
          MessageUtil.setContent(content, buf);
        }
        return buf;
      } finally {
        used = true;
        ReferenceCountUtil.release(content);
      }
    }
  }
}
