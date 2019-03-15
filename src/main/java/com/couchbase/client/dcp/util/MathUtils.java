package com.couchbase.client.dcp.util;

/**
 * Various math utility methods, also backports from later JDK versions.
 *
 * @author Sergey Avseyev
 * @since 0.10.0
 */
public class MathUtils {
  /**
   * Compares two numbers as unsigned 64-bit integers.
   *
   * @param x left operand
   * @param y right operand
   * @return true if x < y
   */
  public static boolean lessThanUnsigned(long x, long y) {
    return Long.compareUnsigned(x, y) < 0;
  }

  /**
   * Backport of {@code Long.compare} from Java 7.
   *
   * @deprecated In favor of {@link Long#compare(long, long)}
   */
  @Deprecated
  public static int compareLong(long x, long y) {
    return Long.compare(x, y);
  }

  /**
   * Backport of {@code Long.compareUnsigned} from Java 8.
   *
   * @deprecated In favor of {@link Long#compareUnsigned(long, long)}
   */
  @Deprecated
  public static int compareUnsignedLong(long x, long y) {
    return Long.compareUnsigned(x, y);
  }
}
