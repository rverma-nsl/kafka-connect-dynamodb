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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

public class DynamoDbSourceConnector extends SourceConnector {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private ConnectorConfig config;
  private Map<Shard, TableDescription> streamShards;

  @Override
  public Class<? extends Task> taskClass() {
    return DynamoDbSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return ConnectorUtils.groupPartitions(new ArrayList<>(streamShards.keySet()), maxTasks)
        .stream()
        .map(
            (List<Shard> taskShards) -> {
              final Map<String, String> taskConfig = new HashMap<>();
              taskConfig.put(TaskConfig.Keys.REGION, config.region.id());
              taskConfig.put(TaskConfig.Keys.TOPIC_FORMAT, config.topicFormat);
              taskConfig.put(
                  TaskConfig.Keys.SHARDS,
                  taskShards.stream()
                      .map(Shard::shardId)
                      .collect(Collectors.joining(",")));
              taskShards.forEach(
                  (Shard shard) -> {
                    final TableDescription tableDesc = streamShards.get(shard);
                    taskConfig.put(
                        shard.shardId() + "." + TaskConfig.Keys.TABLE,
                        tableDesc.tableName());
                    taskConfig.put(
                        shard.shardId() + "." + TaskConfig.Keys.STREAM_ARN,
                        tableDesc.latestStreamArn());
                  });
              return taskConfig;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void start(Map<String, String> props) {
    config = new ConnectorConfig(props);
    streamShards = new HashMap<>();

    final DynamoDbClient client;
    final DynamoDbStreamsClient streamsClient;

    if (config.accessKeyId.value().isEmpty() || config.secretKey.value().isEmpty()) {
      client = DynamoDbClient.builder().region(config.region).build();
      streamsClient = DynamoDbStreamsClient.builder().region(config.region).build();
      log.debug("AmazonDynamoDBStreamsClient created with DefaultAWSCredentialsProviderChain");
    } else {
      final AwsCredentials awsCreds =
          AwsBasicCredentials.create(
              config.accessKeyId.value(), config.secretKey.value());
      client =
          DynamoDbClient.builder()
              .region(config.region)
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
              .build();
      streamsClient =
          DynamoDbStreamsClient.builder()
              .region(config.region)
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
              .build();
      log.debug(
          "AmazonDynamoDB clients created with AWS credentials from connector configuration");
    }

    final Set<String> ignoredTables = new HashSet<>();
    final Set<String> consumeTables = new HashSet<>();

    String lastEvaluatedTableName = null;
    do {
      final ListTablesResponse listResult =
          client.listTables(
              ListTablesRequest.builder()
                  .exclusiveStartTableName(lastEvaluatedTableName)
                  .build());

      for (String tableName : listResult.tableNames()) {
        if (!acceptTable(tableName)) {
          ignoredTables.add(tableName);
          continue;
        }

        final TableDescription tableDesc =
            client.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build())
                .table();

        final StreamSpecification streamSpec = tableDesc.streamSpecification();

        if (streamSpec == null || !streamSpec.streamEnabled()) {
          throw new ConnectException(
              String.format(
                  "DynamoDB table `%s` does not have streams enabled",
                  tableName));
        }

        final String streamViewType = streamSpec.streamViewType().name();
        if (!streamViewType.equals(StreamViewType.NEW_IMAGE.name())
            && !streamViewType.equals(StreamViewType.NEW_AND_OLD_IMAGES.name())) {
          throw new ConnectException(
              String.format(
                  "DynamoDB stream view type for table `%s` is %s",
                  tableName, streamViewType));
        }

        final DescribeStreamResponse describeStreamResult =
            streamsClient.describeStream(
                DescribeStreamRequest.builder()
                    .streamArn(tableDesc.latestStreamArn())
                    .build());

        for (Shard shard : describeStreamResult.streamDescription().shards()) {
          streamShards.put(shard, tableDesc);
        }

        consumeTables.add(tableName);
      }

      lastEvaluatedTableName = listResult.lastEvaluatedTableName();
    } while (lastEvaluatedTableName != null);

    log.info("Tables to ignore: {}", ignoredTables);
    log.info("Tables to ingest: {}", consumeTables);

    client.close();
    streamsClient.close();
  }

  private boolean acceptTable(String tableName) {
    return tableName.startsWith(config.tablesPrefix)
        && (config.tablesWhitelist.isEmpty() || config.tablesWhitelist.contains(tableName))
        && !config.tablesBlacklist.contains(tableName);
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return ConnectorConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
    return Version.get();
  }
}
