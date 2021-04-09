package com.couchbase.client.dcp.util;

import com.couchbase.client.dcp.core.time.Delay;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdaptiveDelayTest {

  private static class TestClock implements AdaptiveDelay.TimeProvider {
    private long nanos;

    @Override
    public long nanoTime() {
      return nanos;
    }

    void advance(Duration d) {
      this.nanos += d.toNanos();
    }
  }

  @Test
  void startsCool() {
    final AdaptiveDelay adaptiveDelay = new AdaptiveDelay(Delay.fixed(5, SECONDS), Duration.ofSeconds(10));
    assertEquals(0, adaptiveDelay.calculate().toNanos());
  }

  @Test
  void cooldownEnforced() {
    final TestClock clock = new TestClock();
    clock.advance(Duration.ofSeconds(-1)); // nanoTime can be negative, so start here

    final AdaptiveDelay adaptiveDelay = new AdaptiveDelay(
        Delay.linear(MILLISECONDS, 3, 1, 1), Duration.ofSeconds(1), clock);

    for (int i = 0; i < 3; i++) {
      assertEquals(0, adaptiveDelay.calculate().toMillis());
      assertEquals(1, adaptiveDelay.calculate().toMillis());

      clock.advance(Duration.ofMillis(500)); // not enough to reset cooldown
      assertEquals(2, adaptiveDelay.calculate().toMillis());

      // cooldown expiry is calculated to include the previous delay,
      // so this isn't enough to reset the cooldown either.
      clock.advance(Duration.ofSeconds(1));

      assertEquals(3, adaptiveDelay.calculate().toMillis());
      assertEquals(3, adaptiveDelay.calculate().toMillis()); // upper bound reached

      clock.advance(Duration.ofSeconds(1).plusMillis(3)); // reset cooldown
    }
  }
}
