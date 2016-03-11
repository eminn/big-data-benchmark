/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.benchmark.jet;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

public class HdfsReporter implements Reporter {
    @Override
    public void setStatus(String status) {

    }

    @Override
    public Counters.Counter getCounter(Enum<?> name) {
        return null;
    }

    @Override
    public Counters.Counter getCounter(String group, String name) {
        return null;
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {

    }

    @Override
    public void incrCounter(String group, String counter, long amount) {

    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void progress() {

    }
}
