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

import com.couchbase.client.dcp.config.HostAndPort;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

public class DcpClientMetrics {
  private final EventCounter reconfigure;
  private final EventCounter addChannel;
  private final EventCounter removeChannel;
  private final LongAdder scheduledPollingTasks;
  private final MetricsContext ctx;

  public DcpClientMetrics(MetricsContext ctx) {
    this.ctx = requireNonNull(ctx);
    this.reconfigure = ctx.newEventCounter("reconfigure").build();
    this.addChannel = ctx.newEventCounter("add.channel").build();
    this.removeChannel = ctx.newEventCounter("remove.channel").build();
    this.scheduledPollingTasks = ctx.registry().gauge("dcp.scheduled.polling.tasks", new LongAdder());
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
}
