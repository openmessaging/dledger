/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

public class Quota {

    private final int max;

    private final int[] samples;
    private final long[] timeVec;

    private final int window;

    public Quota(int max) {
        this(5, max);
    }
    public Quota(int window, int max) {
        if (window < 5) {
            window = 5;
        }
        this.max = max;
        this.window = window;
        this.samples = new int[window];
        this.timeVec = new long[window];
    }

    private int index(long currTimeMs) {
        return  (int) (second(currTimeMs) % window);
    }

    private long second(long currTimeMs) {
        return currTimeMs / 1000;
    }

    public void sample(int value) {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] != second) {
            timeVec[index] = second;
            samples[index] = value;
        } else {
            samples[index] += value;
        }

    }

    public boolean validateNow() {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] == second) {
            return samples[index] >= max;
        }
        return false;
    }

    public int leftNow() {
        long timeMs = System.currentTimeMillis();
        return (int) ((second(timeMs) + 1) * 1000 - timeMs);
    }
}
