/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.core.state;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality for the {@link AbstractStateMachine}.
 */
public class AbstractStateMachineTest {

  @Test
  public void shouldBeInInitialState() {
    SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);
    assertEquals(LifecycleState.DISCONNECTED, sm.state());
    assertTrue(sm.isState(LifecycleState.DISCONNECTED));
    assertFalse(sm.isState(LifecycleState.CONNECTING));
  }

  @Test
  public void shouldTransitionIntoDifferentState() {
    SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

    sm.transitionState(LifecycleState.CONNECTING);
    assertEquals(LifecycleState.CONNECTING, sm.state());
    assertTrue(sm.isState(LifecycleState.CONNECTING));
  }

  static class SimpleStateMachine extends AbstractStateMachine<LifecycleState> {

    public SimpleStateMachine(LifecycleState initialState) {
      super(initialState);
    }

  }
}
