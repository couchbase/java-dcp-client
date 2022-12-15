/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.dcp.util;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionSetTest {

  @Test
  void allPartitions() {
    assertEquals("0..1023", PartitionSet.allPartitions(1024).format());
  }

  @Test
  void parseAndFormat() {
    check("");
    check("0", 0);
    check("0;1", 0, 1);
    check("0..3", 0, 1, 2, 3);
    check("0;2;3", 0, 2, 3);
    check("0;1;8;9", 0, 1, 8,9);
    check("1..3;5..7", 1, 2, 3, 5, 6, 7);
    check("1;3..6;8", 1, 3, 4, 5, 6, 8);
    check("1;4..6;9..11;14", 1, 4, 5, 6, 9, 10, 11, 14);
    check("1;4..6;9..11;14", 1, 11, 5, 6, 9, 10, 4, 14); // unsorted input

    assertEquals(listOf(1, 2, 3, 4), PartitionSet.parse("1;2;3;4").toList());

    // When parsing, sort and discard duplicates
    assertEquals("3;10..12", PartitionSet.parse("10..12;3;3").format());
    assertEquals(listOf(3, 10, 11), PartitionSet.parse("10..11;3;3").toList());
    assertEquals(listOf(3, 10, 11, 12), PartitionSet.parse("10..11;11..12;3").toList());
    assertEquals("3;10..12", PartitionSet.parse("10..11;11..12;3").format());
  }

  private void check(String expectedFormatted, Integer... values) {
    PartitionSet rangeSet = PartitionSet.from(Arrays.asList(values));
    assertEquals(expectedFormatted, rangeSet.format());

    List<Integer> sortedUnique = new ArrayList<>(new TreeSet<>(Arrays.asList(values)));
    assertEquals(sortedUnique, rangeSet.toList());

    assertEquals(sortedUnique, PartitionSet.parse(expectedFormatted).toList());
  }
}

