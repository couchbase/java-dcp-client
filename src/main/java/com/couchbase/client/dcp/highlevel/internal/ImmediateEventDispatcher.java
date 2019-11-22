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

import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Dispatches events in the same thread that calls the dispatch method.
 */
public class ImmediateEventDispatcher implements EventDispatcher {
  private final DatabaseChangeListener listener;

  public ImmediateEventDispatcher(DatabaseChangeListener listener) {
    this.listener = requireNonNull(listener);
  }

  @Override
  public void dispatch(DatabaseChangeEvent event) {
    event.dispatch(listener);
  }

  @Override
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    return true;
  }

  @Override
  public void gracefulShutdown() {
  }

  @Override
  public void shutdownNow() {
  }
}
