/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * A set of partition IDs, printable in a compact human-readable form suitable for logging.
 * <p>
 * For example, if a bucket has 1024 partitions, the set of all partitions is represented as:
 * <pre>
 * 0..1023
 * </pre>
 * Both endpoints are inclusive.
 * <p>
 * If a set contains multiple ranges, the ranges are delimited by semicolons.
 * For example, here's the set of partitions 0,1,2,7,8,9:
 * <pre>
 * 0..2;7..9
 * </pre>
 * Range syntax is used only when there are 3 or more contiguous elements.
 * For two or fewer contiguous elements, partitions are listed independently.
 * For example, here's the set of partitions 0,1,8,9:
 * <pre>
 * 0;1;8;9
 * </pre>
 */
public final class PartitionSet {
  private static final String RANGE_DELIMITER = ";";

  private static class ClosedIntRange {
    private static final String ENDPOINT_DELIMITER = "..";
    private final int start;
    private final int end;

    public ClosedIntRange(int singleValue) {
      this(singleValue, singleValue);
    }

    public ClosedIntRange(int start, int end) {
      if (start > end) {
        throw new IllegalArgumentException("Start must be <= end, but got start " + start + " and end " + end);
      }

      this.start = start;
      this.end = end;
    }

    public static ClosedIntRange parse(String range) {
      try {
        if (range.contains(ENDPOINT_DELIMITER)) {
          String[] components = range.split(Pattern.quote(ENDPOINT_DELIMITER), -1);
          if (components.length != 2) {
            throw new IllegalArgumentException("Malformed ClosedIntRange. Expected lower..upper but got: " + range);
          }
          return new ClosedIntRange(Integer.parseInt(components[0].trim()), Integer.parseInt(components[1].trim()));
        } else {
          return new ClosedIntRange(Integer.parseInt(range));
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Malformed ClosedIntRange. Expected integer or lower..upper but got: " + range, e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClosedIntRange that = (ClosedIntRange) o;
      return start == that.start && end == that.end;
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, end);
    }

    public List<Integer> toList() {
      List<Integer> result = new ArrayList<>();
      for (int i = start; i <= end; i++) {
        result.add(i);
      }
      return result;
    }

    public String toString() {
      return format();
    }

    public String format() {
      if (start == end) {
        return Integer.toString(start);
      }

      return start == end - 1
          ? start + RANGE_DELIMITER + end
          : start + ENDPOINT_DELIMITER + end;
    }
  }

  private static final PartitionSet EMPTY = new PartitionSet(emptyList());

  private PartitionSet(List<ClosedIntRange> ranges) {
    this.ranges = Collections.unmodifiableList(new ArrayList<>(ranges));
  }

  public static PartitionSet parse(String formatted) {
    formatted = formatted.trim();
    if (formatted.isEmpty()) {
      return EMPTY;
    }

    String[] ranges = formatted.split(Pattern.quote(RANGE_DELIMITER), -1);
    List<Integer> result = new ArrayList<>();

    for (String range : ranges) {
      range = range.trim();
      result.addAll(ClosedIntRange.parse(range).toList());
    }

    return PartitionSet.from(result);
  }

  public static PartitionSet allPartitions(int numPartitions) {
    return new PartitionSet(singletonList(new ClosedIntRange(0, numPartitions - 1)));
  }

  public static PartitionSet from(Collection<Integer> values) {
    if (values.isEmpty()) {
      return EMPTY;
    }

    List<ClosedIntRange> result = new ArrayList<>();
    Set<Integer> sorted = new TreeSet<>(values);

    Iterator<Integer> iter = sorted.iterator();

    int currentRangeStart = iter.next();
    int prev = currentRangeStart;

    while (iter.hasNext()) {
      int current = iter.next();
      if (current == prev + 1) {
        prev = current;
      } else {
        result.add(new ClosedIntRange(currentRangeStart, prev));
        currentRangeStart = current;
        prev = current;
      }
    }

    result.add(new ClosedIntRange(currentRangeStart, prev));
    return new PartitionSet(result);
  }

  private final List<ClosedIntRange> ranges;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionSet that = (PartitionSet) o;
    return ranges.equals(that.ranges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ranges);
  }

  public List<Integer> toList() {
    List<Integer> result = new ArrayList<>();
    for (ClosedIntRange r : ranges) {
      result.addAll(r.toList());
    }
    return result;
  }

  public Set<Integer> toSet() {
    return new LinkedHashSet<>(toList());
  }

  public String format() {
    return ranges.stream()
        .map(ClosedIntRange::format)
        .collect(Collectors.joining(RANGE_DELIMITER));
  }

  @Override
  public String toString() {
    return format();
  }
}
