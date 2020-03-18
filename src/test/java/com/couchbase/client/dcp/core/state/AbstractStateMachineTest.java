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
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

  @Test
  public void shouldSendTransitionToObserver() throws Exception {
    SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

    final CountDownLatch latch = new CountDownLatch(3);
    final List<LifecycleState> states = Collections.synchronizedList(new ArrayList<LifecycleState>());
    sm.states().subscribe(new Action1<LifecycleState>() {
      @Override
      public void call(LifecycleState lifecycleState) {
        states.add(lifecycleState);
        latch.countDown();
      }
    });

    sm.transitionState(LifecycleState.CONNECTING);
    sm.transitionState(LifecycleState.CONNECTED);
    sm.transitionState(LifecycleState.DISCONNECTING);

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    assertEquals(LifecycleState.DISCONNECTED, states.get(0));
    assertEquals(LifecycleState.CONNECTING, states.get(1));
    assertEquals(LifecycleState.CONNECTED, states.get(2));
    assertEquals(LifecycleState.DISCONNECTING, states.get(3));
  }

  @Test
  public void shouldNotReceiveOldTransitions() throws Exception {
    SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

    final CountDownLatch latch = new CountDownLatch(2);
    final List<LifecycleState> states = Collections.synchronizedList(new ArrayList<LifecycleState>());

    sm.transitionState(LifecycleState.CONNECTING);

    sm.states().subscribe(new Action1<LifecycleState>() {
      @Override
      public void call(LifecycleState lifecycleState) {
        states.add(lifecycleState);
        latch.countDown();
      }
    });

    sm.transitionState(LifecycleState.CONNECTED);
    sm.transitionState(LifecycleState.DISCONNECTING);

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    assertEquals(LifecycleState.CONNECTING, states.get(0));
    assertEquals(LifecycleState.CONNECTED, states.get(1));
    assertEquals(LifecycleState.DISCONNECTING, states.get(2));
  }

  class SimpleStateMachine extends AbstractStateMachine<LifecycleState> {

    public SimpleStateMachine(LifecycleState initialState) {
      super(initialState);
    }

  }
}
