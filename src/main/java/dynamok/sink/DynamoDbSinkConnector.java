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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import dynamok.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DynamoDbSinkConnector extends SinkConnector {

    static final MetricRegistry metricRegistry = new MetricRegistry();

    private Map<String, String> props;
    private JmxReporter jmxReporter;
    @Override
    public Class<? extends Task> taskClass() {
        return DynamoDbSinkTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        // Starting JMX reporting
        jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("dynamo-connect").build();
        jmxReporter.start();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        metricRegistry.register(MetricRegistry.name(DynamoDbSinkConnector.class, "taskDefinition"),
                (Gauge<String>) () -> props.toString());
        return Collections.nCopies(maxTasks, props);
    }

    @Override
    public void stop() {
        jmxReporter.stop();
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
