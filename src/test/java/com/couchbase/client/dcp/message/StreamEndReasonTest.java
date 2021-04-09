package com.couchbase.client.dcp.message;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class StreamEndReasonTest {
  @Test
  void canCreateUnknown() {
    final int unknownCode = Integer.MAX_VALUE;
    StreamEndReason unknown = StreamEndReason.of(unknownCode);
    StreamEndReason sameAgain = StreamEndReason.of(unknownCode);
    assertEquals(unknown, sameAgain);
    assertEquals(unknownCode, unknown.value());
  }

  @Test
  void canCompareRecognizedUsingIdentity() {
    assertSame(StreamEndReason.of(0), StreamEndReason.OK);
  }
}
