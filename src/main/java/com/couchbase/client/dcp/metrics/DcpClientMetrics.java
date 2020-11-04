/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.metrics;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.state.LifecycleState;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DcpClientMetrics {
  private final EventCounter reconfigure;
  private final EventCounter addChannel;
  private final EventCounter removeChannel;
  private final LongAdder scheduledPollingTasks;
  private final MultiGauge connectionStatus;
  private final MetricsContext ctx;

  public DcpClientMetrics(MetricsContext ctx) {
    this.ctx = requireNonNull(ctx);
    this.reconfigure = ctx.newEventCounter("reconfigure").build();
    this.addChannel = ctx.newEventCounter("add.channel").build();
    this.removeChannel = ctx.newEventCounter("remove.channel").build();
    this.scheduledPollingTasks = ctx.registry().gauge("dcp.scheduled.polling.tasks", new LongAdder());
    this.connectionStatus = MultiGauge.builder("dcp.connection.status").register(ctx.registry());
  }

  public void incrementReconfigure() {
    reconfigure.increment();
  }

  public void incrementAddChannel() {
    addChannel.increment();
  }

  public void incrementRemoveChannel() {
    removeChannel.increment();
  }

  public LongAdder scheduledPollingTasks() {
    return scheduledPollingTasks;
  }

  public DcpChannelMetrics channelMetrics(HostAndPort address) {
    return new DcpChannelMetrics(ctx.withTags(Tags.of("remote", address.format())));
  }

  /**
   * Updates the "connection status" gauge for all channels.
   * <p>
   * Micrometer gauges are finicky about only being registered once,
   * which could cause problems if we were to naively register the gauge
   * as part of the channel metrics, since it's possible for a channel to come and go.
   * <p>
   * A MultiGauge does not suffer from this limitation, so let's use that instead,
   * and overwrite the previous measurements each time there's a connection state change.
   * It's also an easy way to clean up gauges for channels that are no longer part of the cluster.
   */
  public synchronized void updateConnectionStatus(Collection<DcpChannel> channels) {
    List<MultiGauge.Row<?>> rows = channels.stream()
        .map(channel -> {
          Tags tags = Tags.of("remote", channel.address().format());
          double value = channel.isState(LifecycleState.CONNECTED) ? 1 : 0;
          return MultiGauge.Row.of(tags, value);
        })
        .collect(toList());

    final boolean OVERWRITE = true;
    connectionStatus.register(rows, OVERWRITE);
  }
}
