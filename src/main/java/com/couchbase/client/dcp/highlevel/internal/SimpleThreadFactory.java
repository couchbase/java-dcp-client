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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class SimpleThreadFactory implements ThreadFactory {
  private final AtomicInteger nextId = new AtomicInteger();
  private final String baseName;
  private final Consumer<Thread> customizer;

  public SimpleThreadFactory(String baseName, Consumer<Thread> customizer) {
    this.baseName = requireNonNull(baseName);
    this.customizer = requireNonNull(customizer);
  }

  @Override
  public Thread newThread(Runnable r) {
    requireNonNull(r);

    Thread t = new Thread(r);
    t.setName(baseName + nextId.getAndIncrement());
    customizer.accept(t);
    return t;
  }
}
