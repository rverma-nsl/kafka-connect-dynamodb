/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package dynamok.transformers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nsl.logical.model.MessageHolder;
import com.nsl.logical.model.TransactionDto;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransactionTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Insert a random UUID into a connect record";
    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PURPOSE = "Convert transRequet to dynamo Item";

    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public R apply(R record) {
        return applySchemaless(record);
    }

    private R applySchemaless(R record) {
        try {
            MessageHolder<TransactionDto> msg = MAPPER.readValue(record.value().toString(), new TypeReference<>() {
            });
            String transactionId = msg.getData().getTransactionId();
            String uniqueID = CompressionUtils.getTransactionUniqueId(msg.getUserContext().getTenantId(), transactionId, msg.getData().getGsiContextualID());

            final Map<String, Object> item = new HashMap<>();
            item.put("id", uniqueID);
            item.put("containerCuName", msg.getData().getContainerCuName());
            item.put("gsiContextualId", msg.getData().getGsiContextualID());
            item.put("status", msg.getData().getTransactionStatus());
            item.put("transId", transactionId);
            item.put("transType", msg.getData().getTranType());
            item.put("userId", msg.getUserContext().getUserId());
            item.put("payload", CompressionUtils.compressString(MAPPER.writeValueAsString(msg.getData())));
            return newRecord(record, null, item);
        } catch (IOException e) {
            throw new InvalidRecordException("No msg map");
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, updatedSchema, updatedValue, record.timestamp());
    }
}


