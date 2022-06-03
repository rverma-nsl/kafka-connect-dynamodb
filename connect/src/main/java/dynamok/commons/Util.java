package dynamok.commons;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dynamok.sink.DynamoConnectMetaData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Project: kafka-connect-dynamodb
 * Author: shivamsharma
 * Date: 9/22/17.
 */
public final class Util {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static KafkaProducer<String, String> getKafkaProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Dynamo Connector Error Pipeline");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static Map<String, Object> jsonToMap(String json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<>() {
        });
    }

    public static Map<String, Object> jsonToMap(JsonNode json) throws IOException {
        return MAPPER.convertValue(json, new TypeReference<>() {
        });
    }

    public static DynamoConnectMetaData mapToDynamoConnectMetaData(Map<String, Object> map) {
        return MAPPER.convertValue(map, DynamoConnectMetaData.class);
    }

    public static ValidJson isValidJson(String json) {
        try {
            return new ValidJson(MAPPER.readTree(json), true);
        } catch (JacksonException e) {
            return new ValidJson(null, false);
        }
    }

    public static class ValidJson {
        public JsonNode node;
        public boolean isJson;

        public ValidJson(JsonNode node, boolean isJson) {
            this.node = node;
            this.isJson = isJson;
        }
    }


    private Util() {
    }
}
