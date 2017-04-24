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
     * More information at http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
     * "Truth table" for an unsigned comparison x < y using signed arithmetic
     *
     * <table>
     *   <tr><td></td><td colspan="2">Top bit of x</td></tr>
     *   <tr>Top bit of y<td></td><td>0</td><td>1</td></tr>
     *   <tr>0<td></td><td>x < y (signed comparison)</td><td>false</td></tr>
     *   <tr>1<td></td><td>0</td><td>true</td>x < y (signed comparison)</tr>
     * </table>
     *
     * @param x first number
     * @param y second numer
     * @return true if x < y
     */
    public static boolean lessThanUnsigned(long x, long y) {
        return (x < y) ^ ((x < 0) != (y < 0));
    }
}
