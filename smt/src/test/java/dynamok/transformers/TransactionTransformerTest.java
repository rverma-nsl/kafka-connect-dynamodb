/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynamok.transformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TransactionTransformerTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String TXN = "{\n" +
            "  \"data\": {\n" +
            "    \"executionState\": [\n" +
            "      {\n" +
            "        \"containerCuId\": 1664299259149,\n" +
            "        \"referenceContainerCuId\": 1664299259149,\n" +
            "        \"currentCuId\": 1442965838479,\n" +
            "        \"currentContextualId\": \"GS1664299259149.CU1442965838479_193897867197\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"masterTransactionIdRecords\": {},\n" +
            "    \"containerCuDisplayName\": \"Management of Logistics\",\n" +
            "    \"transactionId\": \"730159305639\",\n" +
            "    \"triggerCuId\": 36987852951,\n" +
            "    \"containerCuId\": 1664299259149,\n" +
            "    \"containerCuName\": \"Management of Logistics\",\n" +
            "    \"transactionStatus\": \"TRIGGERED\",\n" +
            "    \"triggerCuName\": \"wareHousingInfo_Fetch\",\n" +
            "    \"dateTime\": 1653496810058,\n" +
            "    \"assignedUserId\": \"959926885238\",\n" +
            "    \"assignedStatus\": \"ASSIGNED\",\n" +
            "    \"startTime\": 1653496809999,\n" +
            "    \"isUpdateAssigneeApplicable\": false,\n" +
            "    \"id\": 730159305639,\n" +
            "    \"guid\": \"c02f63a3-66ca-41cd-b26e-b4eaa87a2978\",\n" +
            "    \"ownerId\": 959926885238,\n" +
            "    \"createdAt\": 1653496809999,\n" +
            "    \"createdBy\": 959926885238,\n" +
            "    \"updatedAt\": 1653496810535,\n" +
            "    \"updatedBy\": 959926885238,\n" +
            "    \"orgUnitId\": 280088328566\n" +
            "  },\n" +
            "  \"userContext\": {\n" +
            "    \"tenantId\": \"astestm302\",\n" +
            "    \"userId\": 959926885238,\n" +
            "    \"emailId\": \"user3@test.com\",\n" +
            "    \"orgUnitId\": 280088328566\n" +
            "  },\n" +
            "  \"logEventTime\": 1653496810547,\n" +
            "  \"methodName\": \"save\"\n" +
            "}";

    private static final String TXN_DYNAMO = "{\n" +
            "  \"transType\" : null,\n" +
            "  \"payload\" : \"H4sIAAAAAAAAAA3QyQJjMAAA0A9y0FqKwxzEMqXUWjS3krTWEpFavn7mfcLD7p496MKEQ494N9ehwqLYtZpKDKqzWUQyM68kaYx7mSVNRbo1wskaGnwvunQ7EWCzbIMiGtBcK+Ow7LlmUSi7Q2v7sjOI9HWK+7jyp5N5/RoU9jObt4npkjhFjmOMIzQKp7nX349wwaBJUsqAQZ3bzcqswMl1zcCRIEt73o2LP30RUHqJb2mM+MLqjq3ubS4NLfx7Lehv3e+a1X96YKiWlO5eDu6mK6Qpfin8LIWPeJkagfs8/+bL3hgpFti7JzknmFkrLVa9Ps19Gl/iyFW1eLMQXAQ+PDtBlmT+YMKw14mK9oPSZsij3eMeOh+r3r7qNbckWk63L5Xe9soL53oLU/9o1ScJi2kSyq2VtY0AbpolboYFy7WLedjtzVCOYq7si4J4rrWPa4KUreGnjsEjvI58E466F2ll2nKOR/4PfbH540Stk2YA6lNZawEfxxBf1k83W2oCx9uNtnWiIKtMseucKP4FW/kLSM+Z3NSNBNtec9cRktUFiOQ9MQa23mfk8REPslQZS9fnKREkPbic3znybACzC+608X0MDwX7NHXLBU0AUibkzUcVW8mMZckMnT//AAGBAkVIAgAA\",\n" +
            "  \"transId\" : \"730159305639\",\n" +
            "  \"containerCuName\" : \"Management of Logistics\",\n" +
            "  \"id\" : \"astestm302_730159305639\",\n" +
            "  \"gsiContextualId\" : null,\n" +
            "  \"userId\" : 959926885238,\n" +
            "  \"status\" : \"TRIGGERED\"\n" +
            "}";
    private static final TransactionTransformer<SourceRecord> xform = new TransactionTransformer<>();

    @AfterAll
    public static void tearDown() {
        xform.close();
    }

    @Test
    public void topLevelStructRequired() {
        Assertions.assertThrows(DataException.class, () -> {
            xform.configure(Collections.singletonMap("demo", "demo"));
            xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
        });
    }

    @Test
    public void transDtoToDynamo() {
        xform.configure(Collections.singletonMap("demo", "demo"));
        try {
            final SourceRecord record = new SourceRecord(null, null, "test", 0,
                    null, TXN);
            final SourceRecord transformedRecord = xform.apply(record);
            assertEquals(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(transformedRecord.value()), TXN_DYNAMO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}