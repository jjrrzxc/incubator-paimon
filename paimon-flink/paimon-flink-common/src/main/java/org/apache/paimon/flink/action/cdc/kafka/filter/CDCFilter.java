/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.kafka.filter;

import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import org.apache.flink.api.common.functions.RichFilterFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CDCFilter extends RichFilterFunction<RichCdcMultiplexRecord> {
    private final List<String> databases;

    public CDCFilter(Optional<String> dbs) {
        databases = Arrays.asList(dbs.orElseGet(() -> "*"));
    }

    @Override
    public boolean filter(RichCdcMultiplexRecord value) throws Exception {
        if (value.primaryKeys() == null || value.primaryKeys().isEmpty()) {
            return false;
        }
        if (!databases.contains("*") && !databases.contains(value.originalDatabase())) {
            return false;
        }
        return true;
    }
}
