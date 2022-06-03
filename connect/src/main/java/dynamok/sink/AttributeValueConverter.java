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

import dynamok.commons.Util;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public final class AttributeValueConverter {

    public static final AttributeValue NULL_VALUE = AttributeValue.fromNul(true);

    public static AttributeValue toAttributeValue(Schema schema, Object value) {
        if (value == null) {
            if (schema.defaultValue() != null) {
                value = schema.defaultValue();
            } else if (schema.isOptional()) {
                return NULL_VALUE;
            } else {
                throw new DataException("null value for non-optional schema with no default value");
            }
        }

        if (schema.name() != null && schema.name().equals(Decimal.LOGICAL_NAME)) {
            return AttributeValue.fromN(value.toString());
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                return AttributeValue.fromN(value.toString());
            case BOOLEAN:
                return AttributeValue.fromBool((boolean) value);
            case STRING:
                return AttributeValue.fromS((String) value);
            case BYTES:
                return AttributeValue.fromB(toByteBuffer(value));
            case ARRAY: {
                return AttributeValue.fromL(((List<?>) value).stream().map(item -> toAttributeValue(schema.valueSchema(), item)).collect(Collectors.toList()));
            }
            case MAP: {
                if (schema.keySchema().isOptional()) {
                    throw new DataException("MAP key schema must not be optional");
                }
                if (!schema.keySchema().type().isPrimitive()) {
                    throw new DataException("MAP key schema must be of primitive type");
                }
                final Map<?, ?> sourceMap = (Map<?, ?>) value;
                final Map<String, AttributeValue> attributesMap = new HashMap<>(sourceMap.size());
                for (Map.Entry<?, ?> e : sourceMap.entrySet()) {
                    attributesMap.put(primitiveAsString(nullFallback(e.getKey(), schema.keySchema().defaultValue())), toAttributeValue(schema.valueSchema(), e.getValue()));
                }
                return AttributeValue.fromM(attributesMap);
            }
            case STRUCT: {
                final Struct struct = (Struct) value;
                final List<Field> fields = schema.fields();
                final Map<String, AttributeValue> attributesMap = new HashMap<>(fields.size());
                for (Field field : fields) {
                    attributesMap.put(field.name(), toAttributeValue(field.schema(), struct.get(field)));
                }
                return AttributeValue.fromM(attributesMap);
            }
            default:
                throw new DataException("Unknown Schema.Type: " + schema.type());
        }
    }

    public static AttributeValue toAttributeValueSchemaless(Object value) {
        if (value == null) {
            return NULL_VALUE;
        }
        if (value instanceof Number) {
            return AttributeValue.fromN(value.toString());
        }
        if (value instanceof Boolean) {
            return AttributeValue.fromBool((Boolean) value);
        }
        if (value instanceof String) {
            try {
                Util.ValidJson json = Util.isValidJson((String) value);
                if (json.isJson) {
                    final Map<String, Object> sourceMap = Util.jsonToMap(json.node);
                    final Map<String, AttributeValue> attributesMap = new HashMap<>(sourceMap.size());
                    for (Map.Entry<String, Object> e : sourceMap.entrySet()) {
                        //Ignoring null & empty strings and keys starting with __
                        if (e.getValue() != null &&
                                (!(e.getValue() instanceof String) || e.getValue() != "") &&
                                e.getKey() != null && !e.getKey().startsWith("__")) {
                            attributesMap.put(primitiveAsString(e.getKey()), toAttributeValueSchemaless(e.getValue()));
                        }
                    }
                    return AttributeValue.fromM(attributesMap);
                } else {
                    return AttributeValue.fromS((String) value);
                }
            } catch (IOException e) {
                throw new DataException("Unsupported Set element type: " + e.getMessage());
            }
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return AttributeValue.fromB(toByteBuffer(value));
        }
        if (value instanceof List) {
            // We could have treated it as NS/BS/SS if the list is homogeneous and a compatible type, but can't know for ane empty list
            return AttributeValue.fromL(((List<?>) value).stream().map(AttributeValueConverter::toAttributeValueSchemaless).collect(Collectors.toList()));
        }
        if (value instanceof Set) {
            final Set<?> set = (Set<?>) value;
            if (set.isEmpty()) {
                return NULL_VALUE;
            }
            final Object firstItem = ((Iterator<?>) set.iterator()).next();
            if (firstItem instanceof String) {
                return AttributeValue.fromSs(set.stream().map(Object::toString).collect(Collectors.toList()));
            }
            if (firstItem instanceof Number) {
                return AttributeValue.fromNs(set.stream().map(Object::toString).collect(Collectors.toList()));
            }
            if (firstItem instanceof byte[] || firstItem instanceof ByteBuffer) {
                return AttributeValue.fromBs(set.stream().map(AttributeValueConverter::toByteBuffer).collect(Collectors.toList()));
            }
            throw new DataException("Unsupported Set element type: " + firstItem.getClass());
        }
        if (value instanceof Map) {
            final Map<?, ?> sourceMap = (Map<?, ?>) value;
            final Map<String, AttributeValue> attributesMap = new HashMap<>(sourceMap.size());
            for (Map.Entry<?, ?> e : sourceMap.entrySet()) {
                //Ignoring null & empty strings and keys starting with __
                if (e.getValue() != null && (!(e.getValue() instanceof String) || e.getValue() != "") && e.getKey() != null && (!(e.getKey() instanceof String) || !((String) e.getKey()).startsWith("__"))) {
                    attributesMap.put(primitiveAsString(e.getKey()), toAttributeValueSchemaless(e.getValue()));
                }
            }
            return AttributeValue.fromM(attributesMap);
        }
        throw new DataException("Unsupported value type: " + value.getClass());
    }

    private static Object nullFallback(Object x, Object fallback) {
        return x != null ? x : fallback;
    }

    private static String primitiveAsString(Object value) {
        if (value instanceof Number || value instanceof Boolean || value instanceof String) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return Base64.getEncoder().encode((ByteBuffer) value).asCharBuffer().toString();
        }
        throw new DataException("Not a primitive: " + value.getClass());
    }

    private static SdkBytes toByteBuffer(Object bytesValue) {
        if (bytesValue instanceof byte[]) {
            return SdkBytes.fromByteArray((byte[]) bytesValue);
        } else if (bytesValue instanceof ByteBuffer) {
            return SdkBytes.fromByteBuffer((ByteBuffer) bytesValue);
        } else {
            throw new DataException("Invalid bytes value of type: " + bytesValue.getClass());
        }
    }

    private AttributeValueConverter() {
    }

}
