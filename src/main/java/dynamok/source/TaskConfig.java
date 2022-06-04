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

import org.apache.kafka.common.config.ConfigException;
import software.amazon.awssdk.regions.Region;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TaskConfig {

    enum Keys {
        ;

        static final String REGION = "region";
        static final String ACCESS_KEY_ID = "access.key.id";
        static final String SECRET_KEY = "secret.key";
        static final String TOPIC_FORMAT = "topic.format";
        static final String SHARDS = "shards";
        static final String TABLE = "table";
        static final String STREAM_ARN = "stream.arn";
    }

    private final Map<String, String> props;

    final Region region;
    final String accessKeyId;
    final String secretKey;
    final String topicFormat;
    final List<String> shards;

    TaskConfig(Map<String, String> props) {
        this.props = props;

        region = Region.of(getValue(Keys.REGION));
        accessKeyId = getValue(Keys.ACCESS_KEY_ID, "");
        secretKey = getValue(Keys.SECRET_KEY, "");
        topicFormat = getValue(Keys.TOPIC_FORMAT);
        shards = Arrays.stream(getValue(Keys.SHARDS).split(",")).filter(shardId -> !shardId.isEmpty()).collect(Collectors.toList());
    }

    String tableForShard(String shardId) {
        return getValue(shardId + "." + Keys.TABLE);
    }

    String streamArnForShard(String shardId) {
        return getValue(shardId + "." + Keys.STREAM_ARN);
    }

    private String getValue(String key) {
        final String value = props.get(key);
        if (value == null) {
            throw new ConfigException(key, "Missing task configuration");
        }
        return value;
    }

    private String getValue(String key, String defaultValue) {
        return props.getOrDefault(key, defaultValue);
    }

}
