/*
 * Copyright 2018 Couchbase, Inc.
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

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A {@code boolean} array in which elements may be updated atomically.
 */
public class AtomicBooleanArray {
    private final AtomicIntegerArray array;

    /**
     * Creates a new AtomicBooleanArray of the given length, with all
     * elements initially false.
     *
     * @param length the length of the array
     */
    public AtomicBooleanArray(int length) {
        // A more efficient implementation could pack 32 booleans into a each integer,
        // but for now let's do something simple so there's nowhere for bugs to hide.
        array = new AtomicIntegerArray(length);
    }

    public boolean get(int index) {
        return array.get(index) == 1;
    }

    public void set(int index, boolean value) {
        array.set(index, value ? 1 : 0);
    }

    public int length() {
        return array.length();
    }
}
