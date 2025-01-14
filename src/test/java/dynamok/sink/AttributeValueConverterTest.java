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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class AttributeValueConverterTest {
  private static final String ATTRIBUTE_JSON =
      "{"
          + "  \"byte\": 1,"
          + "  \"short\": 2,"
          + "  \"int\": 3,"
          + "  \"long\": 4,"
          + "  \"float\": 5.1,"
          + "  \"double\": 6.2,"
          + "  \"decimal\": 7.3,"
          + "  \"bool\": true,"
          + "  \"string\": \"test\","
          + "  \"empty_list\": [],"
          + "  \"string_set\": ["
          + "    \"a\","
          + "    \"b\","
          + "    \"c\""
          + "  ],"
          + "  \"number_set\": ["
          + "    1,"
          + "    2,"
          + "    3"
          + "  ],"
          + "  \"map\": {"
          + "    \"key\": \"value\""
          + "  }"
          + "}";

  private static final String TXN =
      "{\n"
          + "  \"data\": {\n"
          + "    \"executionState\": [\n"
          + "      {\n"
          + "        \"containerCuId\": 1664299259149,\n"
          + "        \"referenceContainerCuId\": 1664299259149,\n"
          + "        \"currentCuId\": 1442965838479,\n"
          + "        \"currentContextualId\": \"GS1664299259149.CU1442965838479_193897867197\"\n"
          + "      }\n"
          + "    ],\n"
          + "    \"masterTransactionIdRecords\": {},\n"
          + "    \"containerCuDisplayName\": \"Management of Logistics\",\n"
          + "    \"transactionId\": \"730159305639\",\n"
          + "    \"triggerCuId\": 36987852951,\n"
          + "    \"containerCuId\": 1664299259149,\n"
          + "    \"containerCuName\": \"Management of Logistics\",\n"
          + "    \"transactionStatus\": \"TRIGGERED\",\n"
          + "    \"triggerCuName\": \"wareHousingInfo_Fetch\",\n"
          + "    \"dateTime\": 1653496810058,\n"
          + "    \"assignedUserId\": \"959926885238\",\n"
          + "    \"assignedStatus\": \"ASSIGNED\",\n"
          + "    \"startTime\": 1653496809999,\n"
          + "    \"isUpdateAssigneeApplicable\": false,\n"
          + "    \"id\": 730159305639,\n"
          + "    \"guid\": \"c02f63a3-66ca-41cd-b26e-b4eaa87a2978\",\n"
          + "    \"ownerId\": 959926885238,\n"
          + "    \"createdAt\": 1653496809999,\n"
          + "    \"createdBy\": 959926885238,\n"
          + "    \"updatedAt\": 1653496810535,\n"
          + "    \"updatedBy\": 959926885238,\n"
          + "    \"orgUnitId\": 280088328566\n"
          + "  },\n"
          + "  \"userContext\": {\n"
          + "    \"tenantId\": \"astestm302\",\n"
          + "    \"userId\": 959926885238,\n"
          + "    \"emailId\": \"user3@test.com\",\n"
          + "    \"orgUnitId\": 280088328566\n"
          + "  },\n"
          + "  \"logEventTime\": 1653496810547,\n"
          + "  \"methodName\": \"save\"\n"
          + "}";

  @Test
  public void schemalessJsonConversion() {
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
    assertEquals(
        Arrays.asList(
            AttributeValue.fromS("a"),
            AttributeValue.fromS("b"),
            AttributeValue.fromS("c")),
        attributeMap.get("string_set").l());
    assertEquals(
        Arrays.asList(
            AttributeValue.fromN("1"),
            AttributeValue.fromN("2"),
            AttributeValue.fromN("3")),
        attributeMap.get("number_set").l());
    assertEquals(
        ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
  }

  @Test
  public void schemalessObjectConversion() {
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
                    .put("byte_array", new byte[] {42})
                    .put("byte_buffer", ByteBuffer.wrap(new byte[] {42}))
                    .put("list", Arrays.asList(1, 2, 3))
                    .put("empty_set", ImmutableSet.of())
                    .put("string_set", ImmutableSet.of("a", "b", "c"))
                    .put("number_set", ImmutableSet.of(1, 2, 3))
                    .put("bytes_set", ImmutableSet.of(new byte[] {42}))
                    .put("map", ImmutableMap.of("key", "value"))
                    .build())
            .m();
    assertEquals("1", attributeMap.get("byte").n());
    assertEquals("2", attributeMap.get("short").n());
    assertEquals("3", attributeMap.get("int").n());
    assertEquals("4", attributeMap.get("long").n());
    assertEquals("5.1", attributeMap.get("float").n());
    assertEquals("6.2", attributeMap.get("double").n());
    assertEquals("7.3", attributeMap.get("decimal").n());
    assertTrue(attributeMap.get("bool").bool());
    assertEquals("test", attributeMap.get("string").s());
    assertEquals(
        ByteBuffer.wrap(new byte[] {42}),
        attributeMap.get("byte_array").b().asByteBuffer());
    assertEquals(
        Arrays.asList(
            AttributeValue.fromN("1"),
            AttributeValue.fromN("2"),
            AttributeValue.fromN("3")),
        attributeMap.get("list").l());
    assertTrue(attributeMap.get("empty_set").nul());
    assertEquals(Arrays.asList("a", "b", "c"), attributeMap.get("string_set").ss());
    assertEquals(Arrays.asList("1", "2", "3"), attributeMap.get("number_set").ns());
    assertEquals(
        List.of(ByteBuffer.wrap(new byte[] {42})),
        attributeMap.get("bytes_set").bs().stream()
            .map(BytesWrapper::asByteBuffer)
            .collect(Collectors.toList()));
    assertEquals(
        ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
  }

    @Test
    public void schemedConversion() {
      Schema nestedStructSchema =
          SchemaBuilder.struct().field("x", SchemaBuilder.STRING_SCHEMA).build();
      Schema schema =
          SchemaBuilder.struct()
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
              .field(
                  "map",
                  SchemaBuilder.map(
                      SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA))
              .field("inner_struct", nestedStructSchema)
              .field("optional_string", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
              .build();

      final Struct struct =
          new Struct(schema)
              .put("int8", (byte) 1)
              .put("int16", (short) 2)
              .put("int32", 3)
              .put("int64", 4L)
              .put("float32", 5.1f)
              .put("float64", 6.2d)
              .put("decimal", new BigDecimal("7.3"))
              .put("bool", true)
              .put("string", "test")
              .put("bytes_a", new byte[] {42})
              .put("bytes_b", ByteBuffer.wrap(new byte[] {42}))
              .put("array", Arrays.asList(1, 2, 3))
              .put("map", ImmutableMap.of("key", "value"))
              .put("inner_struct", new Struct(nestedStructSchema).put("x", "y"));

      final Map<String, AttributeValue> attributeMap =
          AttributeValueConverter.toAttributeValue(schema, struct).m();
      assertEquals("1", attributeMap.get("int8").n());
      assertEquals("2", attributeMap.get("int16").n());
      assertEquals("3", attributeMap.get("int32").n());
      assertEquals("4", attributeMap.get("int64").n());
      assertEquals("5.1", attributeMap.get("float32").n());
      assertEquals("6.2", attributeMap.get("float64").n());
      assertEquals("7.3", attributeMap.get("decimal").n());
      assertTrue(attributeMap.get("bool").bool());
      assertEquals("test", attributeMap.get("string").s());
      assertEquals(ByteBuffer.wrap(new byte[] {42}),
          attributeMap.get("bytes_a").b().asByteBuffer());
      assertEquals(ByteBuffer.wrap(new byte[] {42}),
          attributeMap.get("bytes_b").b().asByteBuffer());
      assertEquals(
          Arrays.asList(
              AttributeValue.fromN("1"),
              AttributeValue.fromN("2"),
              AttributeValue.fromN("3")),
          attributeMap.get("array").l());
      assertEquals(
          ImmutableMap.of("key", AttributeValue.fromS("value")), attributeMap.get("map").m());
      assertEquals(
          ImmutableMap.of("x", AttributeValue.fromS("y")),
          attributeMap.get("inner_struct").m());
      assertTrue(attributeMap.get("optional_string").nul());
    }
}
