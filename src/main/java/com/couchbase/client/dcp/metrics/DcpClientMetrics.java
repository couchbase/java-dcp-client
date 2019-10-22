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

public class DcpClientMetrics {
  private final EventCounter reconfigure;
  private final EventCounter addChannel;
  private final EventCounter removeChannel;

  public DcpClientMetrics(MetricsContext ctx) {
    this.reconfigure = ctx.newEventCounter("reconfigure").build();
    this.addChannel = ctx.newEventCounter("add.channel").build();
    this.removeChannel = ctx.newEventCounter("remove.channel").build();
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
}
