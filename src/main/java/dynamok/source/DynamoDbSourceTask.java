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

package dynamok.source;

import dynamok.Version;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

public class DynamoDbSourceTask extends SourceTask {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private TaskConfig config;
  private DynamoDbStreamsClient streamsClient;
  private List<String> assignedShards;
  private Map<String, String> shardIterators;
  private int currentShardIdx;

  @Override
  public void start(Map<String, String> props) {
    config = new TaskConfig(props);

    if (config.accessKeyId.isEmpty() || config.secretKey.isEmpty()) {
      streamsClient = DynamoDbStreamsClient.builder().region(config.region).build();
      log.debug("AmazonDynamoDBStreamsClient created with DefaultAWSCredentialsProviderChain");
    } else {
      final AwsCredentials awsCreds =
          AwsBasicCredentials.create(config.accessKeyId, config.secretKey);
      streamsClient =
          DynamoDbStreamsClient.builder()
              .region(config.region)
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
              .build();
      log.debug(
          "AmazonDynamoDB clients created with AWS credentials from connector configuration");
    }

    assignedShards = new ArrayList<>(config.shards);
    shardIterators = new HashMap<>(assignedShards.size());
    currentShardIdx = 0;
  }

  @Override
  public List<SourceRecord> poll() {
    // TODO rate limiting?

    if (assignedShards.isEmpty()) {
      throw new ConnectException("No remaining source shards");
    }

    final String shardId = assignedShards.get(currentShardIdx);

    final GetRecordsRequest req =
        GetRecordsRequest.builder()
            .shardIterator(shardIterator(shardId))
            .limit(100)
            .build();

    final GetRecordsResponse rsp = streamsClient.getRecords(req);
    if (rsp.nextShardIterator() == null) {
      log.info(
          "Shard ID `{}` for table `{}` has been closed, it will no longer be polled",
          shardId,
          config.tableForShard(shardId));
      shardIterators.remove(shardId);
      assignedShards.remove(shardId);
    } else {
      log.debug("Retrieved {} records from shard ID `{}`", rsp.records().size(), shardId);
      shardIterators.put(shardId, rsp.nextShardIterator());
    }

    currentShardIdx = (currentShardIdx + 1) % assignedShards.size();

    final String tableName = config.tableForShard(shardId);
    final String topic = config.topicFormat.replace("${table}", tableName);
    final Map<String, String> sourcePartition = sourcePartition(shardId);

    return rsp.records().stream()
        .map(dynamoRecord -> toSourceRecord(sourcePartition, topic, dynamoRecord.dynamodb()))
        .collect(Collectors.toList());
  }

  private SourceRecord toSourceRecord(
      Map<String, String> sourcePartition, String topic, StreamRecord dynamoRecord) {
    return new SourceRecord(
        sourcePartition,
        Collections.singletonMap(Keys.SEQNUM, dynamoRecord.sequenceNumber()),
        topic,
        null,
        RecordMapper.attributesSchema(),
        RecordMapper.toConnect(dynamoRecord.keys()),
        RecordMapper.attributesSchema(),
        RecordMapper.toConnect(dynamoRecord.newImage()),
        dynamoRecord.approximateCreationDateTime().toEpochMilli());
  }

  private String shardIterator(String shardId) {
    String iterator = shardIterators.get(shardId);
    if (iterator == null) {
      final GetShardIteratorRequest req =
          getShardIteratorRequest(
              shardId,
              config.streamArnForShard(shardId),
              storedSequenceNumber(sourcePartition(shardId)));
      iterator = streamsClient.getShardIterator(req).shardIterator();
      shardIterators.put(shardId, iterator);
    }
    return iterator;
  }

  private Map<String, String> sourcePartition(String shardId) {
    return Collections.singletonMap(Keys.SHARD, shardId);
  }

  private String storedSequenceNumber(Map<String, String> partition) {
    final Map<String, Object> offsetMap = context.offsetStorageReader().offset(partition);
    return offsetMap != null ? (String) offsetMap.get(Keys.SEQNUM) : null;
  }

  private GetShardIteratorRequest getShardIteratorRequest(
      String shardId, String streamArn, String seqNum) {

    GetShardIteratorRequest.Builder req =
        GetShardIteratorRequest.builder().shardId(shardId).streamArn(streamArn);
    if (seqNum == null) {
      req.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
    } else {
      req.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
      req.sequenceNumber(seqNum);
    }
    return req.build();
  }

  @Override
  public void stop() {
    if (streamsClient != null) {
      streamsClient.close();
      streamsClient = null;
    }
  }

  @Override
  public String version() {
    return Version.get();
  }

  private enum Keys {
    ;

    static final String SHARD = "shard";
    static final String SEQNUM = "seqNum";
  }
}
