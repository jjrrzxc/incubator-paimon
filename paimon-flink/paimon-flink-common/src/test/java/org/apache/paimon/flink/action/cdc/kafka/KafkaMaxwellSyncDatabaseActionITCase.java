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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link org.apache.paimon.flink.action.cdc.kafka.KafkaSyncDatabaseAction}. */
public class KafkaMaxwellSyncDatabaseActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(600)
    public void testSchemaEvolutionOneTopic() throws Exception {

        final String topic = "schema_evolution";
        boolean writeOne = true;
        int fileCount = 3;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(
                t -> {
                    createTestTopic(t, 1, 1);
                });

        // ---------- Write the maxwell json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka.maxwell/database/schemaevolution/topic"
                                        + i
                                        + "/maxwell-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write maxwell data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "maxwell-json");
        kafkaConfig.put("topic", String.join(";", topics));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        String buckets = String.valueOf(random.nextInt(3) + 1);
        String sinkparallelism = buckets;
        tableConfig.put("bucket", buckets);
        tableConfig.put("sink.parallelism", sinkparallelism);
        tableConfig.put("changelog-producer", "input");
        Map<String, String> catalogconfig = new HashMap<>();
        catalogconfig.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        catalogconfig.put("fs.oss.accessKeyId", "LTAI5tQzQUqz8uTRo9iivgod");
        catalogconfig.put("fs.oss.accessKeySecret", "2k2fg34HRvZTzECLfb7eKOxPFtN3Wf");
        String tableprefix = "ods_dental_";
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableprefix,
                        null,
                        null,
                        null,
                        catalogconfig,
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    private void testSchemaEvolutionImpl(List<String> topics, boolean writeOne, int fileCount)
            throws Exception {
        waitingTables("user_info");

        FileStoreTable table1 = getFileStoreTable("user_info");
        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.VARCHAR(100),
                            DataTypes.VARCHAR(100),
                            DataTypes.INT(),
                            DataTypes.DATE(),
                            DataTypes.DOUBLE(),
                            DataTypes.TIMESTAMP(3),
                            DataTypes.TIMESTAMP(3),
                            DataTypes.VARCHAR(100),
                            DataTypes.VARCHAR(100),
                            DataTypes.VARCHAR(500),
                            DataTypes.DECIMAL(10, 2),
                            DataTypes.VARCHAR(500)
                        },
                        new String[] {
                            "id",
                            "name",
                            "age",
                            "birthday",
                            "money",
                            "update_time",
                            "create_time",
                            "isdeleted",
                            "detail_id",
                            "TenantId",
                            "amount",
                            "aaa"
                        });
        List<String> primaryKeys1 = Collections.singletonList("id");

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka.maxwell/database/schemaevolution/topic"
                                        + i
                                        + "/maxwell-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write maxwell data to Kafka.", e);
            }
        }

        waitForResult(table1, rowType1, primaryKeys1);
    }

    private void waitForResult(FileStoreTable table, RowType rowType, List<String> primaryKeys)
            throws Exception {
        assertThat(table.schema().primaryKeys()).isEqualTo(primaryKeys);

        // wait for table schema to become our expected schema
        while (true) {
            if (rowType.getFieldCount() == table.schema().fields().size()) {
                int cnt = 0;
                for (int i = 0; i < table.schema().fields().size(); i++) {
                    DataField field = table.schema().fields().get(i);
                    boolean sameName = field.name().equals(rowType.getFieldNames().get(i));
                    // boolean sameType = field.type().equals(rowType.getFieldTypes().get(i));
                    if (sameName) {
                        cnt++;
                    }
                }
                if (cnt == rowType.getFieldCount()) {
                    break;
                }
            }
            table = table.copyWithLatestSchema();
            Thread.sleep(1000);
        }

        // wait for data to become expected
        while (true) {
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            List<String> result =
                    getResult(
                            readBuilder.newRead(),
                            plan == null ? Collections.emptyList() : plan.splits(),
                            rowType);
            List<String> sortedActual = new ArrayList<>(result);
            Collections.sort(sortedActual);
            System.out.println(sortedActual);
            Thread.sleep(1000);
        }
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                null,
                Arrays.asList("flink", "paimon_1", "paimon_2"),
                Collections.singletonList("ignore"));
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                null,
                "flink|paimon.+",
                Collections.singletonList("ignore"),
                Arrays.asList("flink", "paimon_1", "paimon_2"));
    }

    @Test
    @Timeout(60)
    public void testIncludingAndExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                "paimon_1",
                Arrays.asList("flink", "paimon_2"),
                Arrays.asList("paimon_1", "ignore"));
    }

    private void includingAndExcludingTablesImpl(
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables)
            throws Exception {
        final String topic1 = "include_exclude" + UUID.randomUUID();
        List<String> topics = Collections.singletonList(topic1);
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the maxwell json into Kafka -------------------

        try {
            writeRecordsToKafka(
                    topics.get(0),
                    readLines("kafka.maxwell/database/include/topic0/maxwell-data-1.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write maxwell data to Kafka.", e);
        }
        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "maxwell-json");
        kafkaConfig.put("topic", String.join(";", topics));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        null,
                        null,
                        includingTables,
                        excludingTables,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        // check paimon tables
        waitingTables(existedTables.toArray(new String[0]));
        assertTableNotExists(notExistedTables);
    }

    // private void assertTableNotExists(List<String> tableNames) {
    //    Catalog catalog = catalog();
    //    for (String tableName : tableNames) {
    //        Identifier identifier = Identifier.create(database, tableName);
    //        assertThat(catalog.tableExists(identifier)).isFalse();
    //    }
    // }

}
