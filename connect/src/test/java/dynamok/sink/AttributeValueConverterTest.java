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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class AttributeValueConverterTest {
    private static final String ATTRIBUTE_JSON = "{" +
            "  \"byte\": 1," +
            "  \"short\": 2," +
            "  \"int\": 3," +
            "  \"long\": 4," +
            "  \"float\": 5.1," +
            "  \"double\": 6.2," +
            "  \"decimal\": 7.3," +
            "  \"bool\": true," +
            "  \"string\": \"test\"," +
            "  \"empty_list\": []," +
            "  \"string_set\": [" +
            "    \"a\"," +
            "    \"b\"," +
            "    \"c\"" +
            "  ]," +
            "  \"number_set\": [" +
            "    1," +
            "    2," +
            "    3" +
            "  ]," +
            "  \"map\": {" +
            "    \"key\": \"value\"" +
            "  }" +
            "}";

    private static final String TXN_DYNAMO = "{\"payload\":\"H4sIAAAAAAAAAAXBx6JrQAAA0A+yYMQEi7dwo0UfPXZEH73L179zCu0O8Qp2+iegWuY6xsK7Kyv4tq6v9gisTRwYCFBwJlM6IxdTjTBpb7XOb7Kz389AaOh5ZNmczrtEHfvpVh6auY7hzdZVivV0OXg117K4qpTsNct11P3lflzA0+53JPwlaIa652WSEw7UzXvVLusQa8bgS883X1XuDDDegHuGTVWbMCJ0cuC4RelsgMqIbhUlIzwH7N+MzaJLhZyxWKY3JYaBUcr0p4Thz9p/qy82NOeYQkGBrDwDXSxY/7q24PEAV4ta9JykqPm7NsElrHWhCmVM7itNpmJhclOXcS8yi0+ldxF4iu5oA27qZHYdUj3b6cVK+vyY76jhYv55wP1pKw0voOJgx12x1jUrqOKgxeAAWj7wmcK/4gNFHw6ioO7NpNkQfVi3I9omZg/qWS55aVxfCxbvtmJAC76dzLkbzfcBy1ykVPZxs1KpE9oOz+YacMJSfTUjkjrqjOBH6IT4ttXMH3I3W/DXG4g4C3hchZgYkmbSZzxdhL9WBcIeqSSQqPMF6g0gHYPmD6tbDLFiVXluP3nRa9E2ChASAlU+SPX97z97VgfkMAIAAA==\",\"transId\":\"730159305639\",\"containerCuName\":\"Management of Logistics\",\"id\":\"astestm302_730159305639\",\"userId\":959926885238,\"status\":\"TRIGGERED\"}";

    @Test
    public void jsonConversion() {
        final Map<String, AttributeValue> attributeMap =
                AttributeValueConverter.toAttributeValueSchemaless(ATTRIBUTE_JSON).m();
        assertEquals("1", attributeMap.get("byte").n());
        assertEquals("2", attributeMap.get("short").n());
        assertEquals("3", attributeMap.get("int").n());
        assertEquals("4", attributeMap.get("long").n());
        assertEquals("5.1", attributeMap.get("float").n());
        assertEquals("6.2", attributeMap.get("double").n());
        assertEquals("7.3", attributeMap.get("decimal").n());
        assertTrue(attributeMap.get("bool").bool());
        assertEquals("test", attributeMap.get("string").s());
        assertEquals(Arrays.asList(), attributeMap.get("empty_list").l());
//        assertEquals(Arrays.asList("a", "b", "c"), attributeMap.get("string_set").l());
//        assertEquals(Arrays.asList("1", "2", "3"), attributeMap.get("number_set").l());
        assertEquals(ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
    }

    @Test
    public void smtJsonConversion() {
        final Map<String, AttributeValue> attributeMap =
                AttributeValueConverter.toAttributeValueSchemaless(TXN_DYNAMO).m();
        assertEquals("959926885238", attributeMap.get("userId").n());
    }

    @Test
    public void schemalessConversion() {
        final Map<String, AttributeValue> attributeMap =
                AttributeValueConverter.toAttributeValueSchemaless(
                        ImmutableMap.builder()
                                .put("byte", (byte) 1)
                                .put("short", (short) 2)
                                .put("int", 3)
                                .put("long", 4L)
                                .put("float", 5.1f)
                                .put("double", 6.2d)
                                .put("decimal", new BigDecimal("7.3"))
                                .put("bool", true)
                                .put("string", "test")
                                .put("byte_array", new byte[]{42})
                                .put("byte_buffer", ByteBuffer.wrap(new byte[]{42}))
                                .put("list", Arrays.asList(1, 2, 3))
                                .put("empty_set", ImmutableSet.of())
                                .put("string_set", ImmutableSet.of("a", "b", "c"))
                                .put("number_set", ImmutableSet.of(1, 2, 3))
                                .put("bytes_set", ImmutableSet.of(new byte[]{42}))
                                .put("map", ImmutableMap.of("key", "value"))
                                .build()
                ).m();
        assertEquals("1", attributeMap.get("byte").n());
        assertEquals("2", attributeMap.get("short").n());
        assertEquals("3", attributeMap.get("int").n());
        assertEquals("4", attributeMap.get("long").n());
        assertEquals("5.1", attributeMap.get("float").n());
        assertEquals("6.2", attributeMap.get("double").n());
        assertEquals("7.3", attributeMap.get("decimal").n());
        assertTrue(attributeMap.get("bool").bool());
        assertEquals("test", attributeMap.get("string").s());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("byte_array").b().asByteBuffer());
        assertEquals(
                Arrays.asList(AttributeValue.fromN("1"), AttributeValue.fromN("2"), AttributeValue.fromN("3")),
                attributeMap.get("list").l()
        );
        assertTrue(attributeMap.get("empty_set").nul());
        assertEquals(Arrays.asList("a", "b", "c"), attributeMap.get("string_set").ss());
        assertEquals(Arrays.asList("1", "2", "3"), attributeMap.get("number_set").ns());
        assertEquals(List.of(ByteBuffer.wrap(new byte[]{42})), attributeMap.get("bytes_set").bs().stream().map(BytesWrapper::asByteBuffer).collect(Collectors.toList()));
        assertEquals(ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
    }

    @Test
    public void schemaedConversion() {
        Schema nestedStructSchema = SchemaBuilder.struct().field("x", SchemaBuilder.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("int8", SchemaBuilder.INT8_SCHEMA)
                .field("int16", SchemaBuilder.INT16_SCHEMA)
                .field("int32", SchemaBuilder.INT32_SCHEMA)
                .field("int64", SchemaBuilder.INT64_SCHEMA)
                .field("float32", SchemaBuilder.FLOAT32_SCHEMA)
                .field("float64", SchemaBuilder.FLOAT64_SCHEMA)
                .field("decimal", Decimal.schema(1))
                .field("bool", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("string", SchemaBuilder.STRING_SCHEMA)
                .field("bytes_a", SchemaBuilder.BYTES_SCHEMA)
                .field("bytes_b", SchemaBuilder.BYTES_SCHEMA)
                .field("array", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA))
                .field("inner_struct", nestedStructSchema)
                .field("optional_string", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct struct = new Struct(schema)
                .put("int8", (byte) 1)
                .put("int16", (short) 2)
                .put("int32", 3)
                .put("int64", 4L)
                .put("float32", 5.1f)
                .put("float64", 6.2d)
                .put("decimal", new BigDecimal("7.3"))
                .put("bool", true)
                .put("string", "test")
                .put("bytes_a", new byte[]{42})
                .put("bytes_b", ByteBuffer.wrap(new byte[]{42}))
                .put("array", Arrays.asList(1, 2, 3))
                .put("map", ImmutableMap.of("key", "value"))
                .put("inner_struct", new Struct(nestedStructSchema).put("x", "y"));

        final Map<String, AttributeValue> attributeMap = AttributeValueConverter.toAttributeValue(schema, struct).m();
        assertEquals("1", attributeMap.get("int8").n());
        assertEquals("2", attributeMap.get("int16").n());
        assertEquals("3", attributeMap.get("int32").n());
        assertEquals("4", attributeMap.get("int64").n());
        assertEquals("5.1", attributeMap.get("float32").n());
        assertEquals("6.2", attributeMap.get("float64").n());
        assertEquals("7.3", attributeMap.get("decimal").n());
        assertTrue(attributeMap.get("bool").bool());
        assertEquals("test", attributeMap.get("string").s());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("bytes_a").b().asByteBuffer());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("bytes_b").b().asByteBuffer());
        assertEquals(
                Arrays.asList(AttributeValue.fromN("1"), AttributeValue.fromN("2"), AttributeValue.fromN("3")),
                attributeMap.get("array").l()
        );
        assertEquals(ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
        assertEquals(ImmutableMap.of("x", AttributeValue.fromS("y")), attributeMap.get("inner_struct").m());
        assertTrue(attributeMap.get("optional_string").nul());
    }

}
