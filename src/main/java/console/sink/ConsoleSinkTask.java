package console.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Project: kafka-connect-dynamodb
 * Author: shivamsharma
 * Date: 9/21/17.
 */
public class ConsoleSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleSinkTask.class);

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting console sink task");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach(p -> LOGGER.info("Record: {}", p));
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void stop() {

    }
}
