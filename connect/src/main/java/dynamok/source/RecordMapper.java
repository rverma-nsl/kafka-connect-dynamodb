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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum RecordMapper {
    ;

    private static final Schema AV_SCHEMA =
            SchemaBuilder.struct()
                    .name("DynamoDB.AttributeValue")
                    .field("S", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("N", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("B", Schema.OPTIONAL_BYTES_SCHEMA)
                    .field("SS", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .field("NS", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .field("BS", SchemaBuilder.array(Schema.BYTES_SCHEMA).optional().build())
                    .field("NULL", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .field("BOOL", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    //      .field("L", "DynamoDB.AttributeValue") -- FIXME https://issues.apache.org/jira/browse/KAFKA-3910
                    //      .field("M", "DynamoDB.AttributeValue") -- FIXME https://issues.apache.org/jira/browse/KAFKA-3910
                    .version(1)
                    .build();

    private static final Schema DYNAMODB_ATTRIBUTES_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, AV_SCHEMA)
                    .name("DynamoDB.Attributes")
                    .version(1)
                    .build();

    public static Schema attributesSchema() {
        return DYNAMODB_ATTRIBUTES_SCHEMA;
    }

    public static Map<String, Struct> toConnect(Map<String, AttributeValue> attributes) {
        Map<String, Struct> connectAttributes = new HashMap<>(attributes.size());
        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            final Struct attributeValueStruct = new Struct(AV_SCHEMA);
            if (attributeValue.s() != null) {
                attributeValueStruct.put("S", attributeValue.s());
            } else if (attributeValue.n() != null) {
                attributeValueStruct.put("N", attributeValue.n());
            } else if (attributeValue.b() != null) {
                attributeValueStruct.put("B", attributeValue.b().asByteBuffer());
            } else if (attributeValue.ss().size() > 0) {
                attributeValueStruct.put("SS", attributeValue.ss());
            } else if (attributeValue.ns().size() > 0) {
                attributeValueStruct.put("NS", attributeValue.ns());
            } else if (attributeValue.bs().size() > 0) {
                attributeValueStruct.put("BS", attributeValue.bs().stream().map(BytesWrapper::asByteBuffer).collect(Collectors.toList()));
            } else if (attributeValue.nul() != null) {
                attributeValueStruct.put("NULL", attributeValue.nul());
            } else if (attributeValue.bool() != null) {
                attributeValueStruct.put("BOOL", attributeValue.bool());
            }
            connectAttributes.put(attributeName, attributeValueStruct);
        }
        return connectAttributes;
    }

}
