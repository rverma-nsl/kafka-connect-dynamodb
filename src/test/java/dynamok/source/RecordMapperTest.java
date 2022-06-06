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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class RecordMapperTest {

    @Test
    public void conversions() {
      final String string = "test";
      final String number = "42";
      final SdkBytes bytes = SdkBytes.fromByteArray(new byte[] {42});
      final boolean bool = true;
      final boolean nullValue = true;
      final List<String> stringList = List.of(string);
      final List<String> numberList = List.of(number);
      final List<SdkBytes> byteList = List.of(bytes);
      final Map<String, Struct> record =
          RecordMapper.toConnect(
              ImmutableMap.<String, AttributeValue>builder()
                  .put("thestring", AttributeValue.fromS(string))
                  .put("thenumber", AttributeValue.fromN(number))
                  .put("thebytes", AttributeValue.fromB(bytes))
                  .put("thestrings", AttributeValue.fromSs(stringList))
                  .put("thenumbers", AttributeValue.fromNs(numberList))
                  .put("thebyteslist", AttributeValue.fromBs(byteList))
                  .put("thenull", AttributeValue.fromNul(true))
                  .put("thebool", AttributeValue.fromBool(bool))
                  .build());
      assertEquals(string, record.get("thestring").get("S"));
      assertEquals(number, record.get("thenumber").get("N"));
      assertEquals(bytes.asByteBuffer(), record.get("thebytes").get("B"));
      assertEquals(Collections.singletonList(string), record.get("thestrings").get("SS"));
      assertEquals(Collections.singletonList(number), record.get("thenumbers").get("NS"));
      assertEquals(
          Stream.of(bytes).map(BytesWrapper::asByteBuffer).collect(Collectors.toList()),
          record.get("thebyteslist").get("BS"));
      assertEquals(nullValue, record.get("thenull").get("NULL"));
      assertEquals(bool, record.get("thebool").get("BOOL"));
    }
}
