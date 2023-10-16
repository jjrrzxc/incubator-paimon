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

package org.apache.paimon.flink.action.cdc.kafka.formats.maxwell;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

public class MaxwellRecordParser extends RecordParser {
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_SQL = "sql";
    private static final String FIELD_MYSQL_TYPE = "column";
    private static final String FIELD_PRIMARY_KEYS = "primary_key_columns";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    private static final String OP_UPDATE = "update";
    private static final String OP_INSERT = "insert";
    private static final String OP_BOOTSTRAP_INSERT = "bootstrap-insert";
    private static final String OP_DELETE = "delete";
    private static final String OP_TABLE_ALTER = "table-alter";
    private static final String OP_TABLE_CREATE = "table-create";
    private static final String OP_TABLE_DROP = "table-drop";
    private static final List<String> OP_DDLS =
            Arrays.asList("table-alter", "table-create", "table-drop");
    private static final List<String> OP_DMLS =
            Arrays.asList(OP_UPDATE, OP_INSERT, OP_DELETE, OP_BOOTSTRAP_INSERT);;

    private final List<ComputedColumn> computedColumns;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // private JsonNode root;
    private String databaseName;
    private String tableName;

    public MaxwellRecordParser(boolean caseSensitive, TableNameConverter tableNameConverter) {
        this(caseSensitive, tableNameConverter, Collections.emptyList());
    }

    public MaxwellRecordParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        super(tableNameConverter, caseSensitive);
        this.computedColumns = computedColumns;
    }

    @Override
    public void flatMap(String value, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = objectMapper.readValue(value, JsonNode.class);
        validateFormat();
        databaseName = extractString(FIELD_DATABASE);
        tableName = tableNameConverter.convert(extractString(FIELD_TABLE));
        if (root.get(FIELD_MYSQL_TYPE) != null && root.get(FIELD_PRIMARY_KEYS) != null) {
            extractRecords().forEach(out::collect);
        }
    }

    protected List<RichCdcMultiplexRecord> extractRecords() {
        if (!isdml()) {
            return Collections.emptyList();
        }
        List<String> primaryKeys = extractPrimaryKeys(FIELD_PRIMARY_KEYS);

        LinkedHashMap<String, String> mySqlFieldTypes = extractFieldTypesFromMySqlType();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        mySqlFieldTypes.forEach(
                (name, type) ->
                        paimonFieldTypes.put(
                                StringUtils.caseSensitiveConversion(name, caseSensitive),
                                MySqlTypeUtils.toDataTypeForMaxwell(type)));

        String type = extractString(FIELD_TYPE);
        ObjectNode data = (ObjectNode) root.get(FIELD_DATA);

        String tenantdefine = super.conf.get("original.tenant.define");
        TextNode tenantnode = (TextNode) data.get(tenantdefine);
        if (!StringUtils.isEmpty(tenantdefine)
                && !primaryKeys.stream()
                        .anyMatch(
                                s ->
                                        org.apache.commons.lang3.StringUtils.containsIgnoreCase(
                                                s, tenantdefine))
                && tenantnode != null
                && !StringUtils.isEmpty(tenantnode.asText())) {
            primaryKeys.add(StringUtils.caseSensitiveConversion(tenantdefine, caseSensitive));
        }
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        Map<String, String> after;
        switch (type) {
            case OP_UPDATE:
                ObjectNode old =
                        root.get(FIELD_OLD) instanceof NullNode
                                ? null
                                : (ObjectNode) root.get(FIELD_OLD);
                after = extractRow(data, mySqlFieldTypes);
                if (old != null) {
                    Map<String, String> before = extractRow(old, mySqlFieldTypes);
                    // fields in "old" (before) means the fields are changed
                    // fields not in "old" (before) means the fields are not changed,
                    // so we just copy the not changed fields into before
                    for (Map.Entry<String, String> entry : after.entrySet()) {
                        if (!before.containsKey(entry.getKey())) {
                            before.put(entry.getKey(), entry.getValue());
                        }
                    }
                    before = caseSensitive ? before : keyCaseInsensitive(before);
                    records.add(
                            new RichCdcMultiplexRecord(
                                    databaseName,
                                    tableName,
                                    paimonFieldTypes,
                                    primaryKeys,
                                    new CdcRecord(RowKind.DELETE, before)));
                }
                after = caseSensitive ? after : keyCaseInsensitive(after);
                records.add(
                        new RichCdcMultiplexRecord(
                                databaseName,
                                tableName,
                                paimonFieldTypes,
                                primaryKeys,
                                new CdcRecord(RowKind.INSERT, after)));
                break;
            case OP_INSERT:
            case OP_BOOTSTRAP_INSERT:
                // fall through
            case OP_DELETE:
                after = extractRow(data, mySqlFieldTypes);
                after = caseSensitive ? after : keyCaseInsensitive(after);
                RowKind kind =
                        (type.equals(OP_INSERT) || type.equals(OP_BOOTSTRAP_INSERT))
                                ? RowKind.INSERT
                                : RowKind.DELETE;
                records.add(
                        new RichCdcMultiplexRecord(
                                databaseName,
                                tableName,
                                paimonFieldTypes,
                                primaryKeys,
                                new CdcRecord(kind, after)));
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + type);
        }

        return records;
    }

    protected Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
        Map<String, String> keyCaseInsensitive = new HashMap<>();
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String fieldName = entry.getKey().toLowerCase();
            checkArgument(
                    !keyCaseInsensitive.containsKey(fieldName),
                    "Duplicate key appears when converting map keys to case-insensitive form. Original map is:\n%s",
                    origin);
            keyCaseInsensitive.put(fieldName, entry.getValue());
        }
        return keyCaseInsensitive;
    }

    private Map<String, String> extractRow(JsonNode record, Map<String, String> mySqlFieldTypes) {
        Map<String, Object> jsonMap =
                objectMapper.convertValue(record, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : mySqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue();
            Object objectValue = jsonMap.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String value = objectValue.toString();

            if (MySqlTypeUtils.isSetType(MySqlTypeUtils.getShortType(mySqlType))
                    || MySqlTypeUtils.isEnumType(MySqlTypeUtils.getShortType(mySqlType))
                    || MySqlTypeUtils.isGeoType(MySqlTypeUtils.getShortType(mySqlType))) {
                throw new RuntimeException(
                        String.format("the type %s has not supported yet", mySqlType));
            }

            resultMap.put(fieldName, value);
        }

        // generate values for computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
        }

        return resultMap;
    }

    private LinkedHashMap<String, String> extractFieldTypesFromMySqlType() {
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();

        JsonNode schema = root.get(FIELD_MYSQL_TYPE);
        Iterator<String> iterator = schema.fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            String fieldType = schema.get(fieldName).asText();
            fieldTypes.put(fieldName, fieldType);
        }

        return fieldTypes;
    }

    // private List<String> extractPrimaryKeys() {
    //    List<String> primaryKeys = new ArrayList<>();
    //    ArrayNode pkNames = (ArrayNode) root.get(FIELD_PRIMARY_KEYS);
    //    pkNames.iterator().forEachRemaining(pk -> primaryKeys.add(toFieldName(pk.asText())));
    //    return primaryKeys;
    // }

    protected String toFieldName(String rawName) {
        return StringUtils.caseSensitiveConversion(rawName, caseSensitive);
    }

    private boolean isdml() {
        return root.get(FIELD_TYPE) != null && OP_DMLS.contains(root.get(FIELD_TYPE).asText());
    }

    private boolean isDdl() {
        return root.get(FIELD_TYPE) != null && OP_DDLS.contains(root.get(FIELD_TYPE).asText());
    }

    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Only supports maxwell-json format,"
                        + "please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_DATABASE), errorMessageTemplate, FIELD_DATABASE);
        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);

        if (isDdl()) {
            checkNotNull(root.get(FIELD_SQL), errorMessageTemplate, FIELD_SQL);
        }
        // else {
        //    checkNotNull(root.get(FIELD_MYSQL_TYPE), errorMessageTemplate, FIELD_MYSQL_TYPE);
        //    checkNotNull(root.get(FIELD_PRIMARY_KEYS), errorMessageTemplate, FIELD_PRIMARY_KEYS);
        // }
    }

    protected String extractString(String key) {
        return root.get(key).asText();
    }

    @Override
    public KafkaSchema getKafkaSchema(String record) {
        // only support database sync
        return null;
    }
}
