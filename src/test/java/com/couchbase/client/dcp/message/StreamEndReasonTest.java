package com.couchbase.client.dcp.message;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamEndReasonTest {
  @Test
  public void canCreateUnknown() {
    final int unknownCode = Integer.MAX_VALUE;
    StreamEndReason unknown = StreamEndReason.of(unknownCode);
    StreamEndReason sameAgain = StreamEndReason.of(unknownCode);
    assertEquals(unknown, sameAgain);
    assertEquals(unknownCode, unknown.value());
  }

  @Test
  public void canCompareRecognizedUsingIdentity() {
    assertSame(StreamEndReason.of(0), StreamEndReason.OK);
  }
}
