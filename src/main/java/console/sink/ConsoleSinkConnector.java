package console.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Project: kafka-connect-dynamodb
 * Author: shivamsharma
 * Date: 9/21/17.
 */
public class ConsoleSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(ConsoleSinkConnector.class);
    private Map<String, String> props;
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        logger.info("Starting Console Sink Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConsoleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, props);
    }

    @Override
    public void stop() {
        logger.info("Stopping Console Sink Connector");
    }

    @Override
    public ConfigDef config() {
        return null;
    }
}
