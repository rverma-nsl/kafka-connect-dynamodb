/*
 * Copyright 2016 Shikhar Bhushan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynamok.sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import dynamok.Version;
import dynamok.commons.Util;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class DynamoDbSinkTask extends SinkTask {

    private final Meter requests = DynamoDbSinkConnector.METRIC_REGISTRY.meter("Requests");
    private final Counter jsonParseException = DynamoDbSinkConnector.METRIC_REGISTRY.counter("JsonParseException");
    private final Counter conditionalCheckFailed = DynamoDbSinkConnector.METRIC_REGISTRY.counter("ConditionalCheckFailed");
    private final Timer requestProcessingTimer = DynamoDbSinkConnector.METRIC_REGISTRY.timer("RequestProcessingTime");

    private enum ValueSource {
        RECORD_KEY {
            @Override
            String topAttributeName(ConnectorConfig config) {
                return config.topKeyAttribute;
            }
        },
        RECORD_VALUE {
            @Override
            String topAttributeName(ConnectorConfig config) {
                return config.topValueAttribute;
            }
        };

        abstract String topAttributeName(ConnectorConfig config);
    }

    private final Logger log = LoggerFactory.getLogger(DynamoDbSinkTask.class);

    private ConnectorConfig config;
    private DynamoDbClient client;
    private KafkaProducer<String, String> producer;

    private final Map<String, String> tableMap = new HashMap<>();

    @Override
    public void start(Map<String, String> props) {
        config = new ConnectorConfig(props);
        producer = Util.getKafkaProducer(config.broker);
        DynamoDbClientBuilder ddb = DynamoDbClient.builder()
                .region(config.region);

        if (config.accessKeyId.value().isEmpty() || config.secretKey.value().isEmpty()) {
            log.debug("AmazonDynamoDBStreamsClient created with DefaultAWSCredentialsProviderChain");
        } else {
            final AwsCredentials awsCreds = AwsBasicCredentials.create(config.accessKeyId.value(), config.secretKey.value());
            ddb = ddb.credentialsProvider(StaticCredentialsProvider.create(awsCreds));
            log.debug("AmazonDynamoDBClient created with AWS credentials from connector configuration");
        }
        client = ddb.build();
        for (String spl : config.tableFormat.split(",")) {
            String[] e = spl.split("=");
            tableMap.put(e[0], e[1]);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void put(Collection<SinkRecord> records) {
        requests.mark(records.size());

        for (final SinkRecord record : records) {
            Timer.Context timerContext = requestProcessingTimer.time();
            try {
                DynamoConnectMetaData dynamoConnectMetaData = null;
                PutItemRequest putItemRequest = toPutRequest(record, dynamoConnectMetaData);
                client.putItem(putItemRequest);
            } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
                log.debug("Write failed with Limit/Throughput Exceeded exception; backing off");
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            } catch (ConditionalCheckFailedException e) {
                conditionalCheckFailed.inc();
                log.debug("Conditional check failed for record: {}", record, e);
                //This is intentional failure for conditional check
            } catch (DynamoDbException e) {
                log.warn("Error in sending data to DynamoDB in record: {}", record, e);
                if (e.awsErrorDetails().errorCode().equalsIgnoreCase("ValidationException")) {
                    producer.send(new ProducerRecord<>(config.errorKafkaTopic, "DynamoDbException" + record.key(), record.value().toString()));
                } else {
                    System.exit(1); // To exit connect completely
                }
            } finally {
                timerContext.stop();
            }
        }
    }

    private PutItemRequest toPutRequest(SinkRecord record, DynamoConnectMetaData dynamoConnectMetaData) {
        final PutItemRequest.Builder pir = PutItemRequest.builder().tableName(tableName(record));
        Map<String, AttributeValue> item = new HashMap<>();
        if (!config.ignoreRecordValue) {
            insert(ValueSource.RECORD_VALUE, record.valueSchema(), record.value(), item);
        }
        if (!config.ignoreRecordKey) {
            insert(ValueSource.RECORD_KEY, record.keySchema(), record.key(), item);
        }
        if (config.kafkaCoordinateNames != null) {
            item.put(config.kafkaCoordinateNames.topic, AttributeValue.fromS(record.topic()));
            item.put(config.kafkaCoordinateNames.partition, AttributeValue.fromN(String.valueOf(record.kafkaPartition())));
            item.put(config.kafkaCoordinateNames.offset, AttributeValue.fromN(String.valueOf(record.kafkaOffset())));
        }
        if (dynamoConnectMetaData != null) {
            pir.conditionExpression(dynamoConnectMetaData.getConditionalExpression())
                    .expressionAttributeValues(AttributeValueConverter.toAttributeValueSchemaless(dynamoConnectMetaData.getConditionalValueMap()).m());
        }
        return pir.build();
    }

    private void insert(ValueSource valueSource, Schema schema, Object value, Map<String, AttributeValue> item) {
        final AttributeValue attributeValue;
        try {
            attributeValue = schema == null
                    ? AttributeValueConverter.toAttributeValueSchemaless(value)
                    : AttributeValueConverter.toAttributeValue(schema, value);
        } catch (DataException e) {
            log.error("Failed to convert record with schema={} value={}", schema, value, e);
            throw e;
        }

        final String topAttributeName = valueSource.topAttributeName(config);
        if (!topAttributeName.isEmpty()) {
            item.put(topAttributeName, attributeValue);
        } else if (attributeValue.m() != null) {
            item.putAll(attributeValue.m());
        } else {
            throw new ConnectException("No top attribute name configured for " + valueSource + ", and it could not be converted to Map: " + attributeValue);
        }
    }

    private String tableName(SinkRecord record) {
        return tableMap.get(record.topic());
//        return config.tableFormat.equalsIgnoreCase("${topic}") ? config.tableFormat.replace("${topic}", record.topic()) : config.tableFormat;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public String version() {
        return Version.get();
    }

}
