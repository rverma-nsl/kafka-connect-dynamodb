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
import java.util.Map;

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

    private static final String TxnDynamo = "{\n" +
            "  \"transType\": \"null\",\n" +
            "  \"payload\": \"eJyVUctu2zAQ/JWAZ7kQRZEifXOc1BGQ5uDHqSiCNbVSCMiUQVJIAsP/XspxUzkBUpRHzszOzO6B4AvqPpjOrgIEJNOfB6I7G8BYdPO+rMiUCpFnSmVc0VwlxGGNDq3G+Zc03bvICmcsj5Dgksm8GGFxAL6EHtqBQxarixHf5psL2SNVTKpCioKqghx/JWQHPqBbO7Ae9FChrJaoO1d5Mj0ck3GPG+P3Lbw+wC5WJD/AQoO7GOGqq6/uu8b4YLQnCQnjYZFZsJRyxVIumDrBpmn+FGZCyULyTHGa/GNnI/R/Igwn6WMZsl6Wi8Xt8vZmnOE86Rkc3nW9N7Ypbd09fsegnyKviudcm4FCBWe5EpKmKZcJAe9NY7HaeHSnkorHpELGKkySv/i7+2y1KhcPJ3MfwIXLqamKLyHGb/aD5exNjbP9vjUatm2k1tB6jJRoNl5oQpp++CM6zWrBgE2E0DDJqa4m20zgZJsjgCwgi2eP5t2zfUs8DhyX6zD6VrPwKdMZuX79KOlPUS8kNOWMvyOfJZ1rNtaEwT6TaSolyyQX4vgbzOj9Lg==\",\n" +
            "  \"transId\": \"730159305639\",\n" +
            "  \"containerCuName\": \"Management of Logistics\",\n" +
            "  \"id\": \"astestm302_730159305639_730159305639\",\n" +
            "  \"gsiContextualId\": \"null\",\n" +
            "  \"status\": \"TRIGGERED\",\n" +
            "  \"userId\": \"959926885238\"\n" +
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
                    null, mapper.readValue(TXN, Map.class));
            final SourceRecord transformedRecord = xform.apply(record);
            assertEquals(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(transformedRecord.value()), TxnDynamo.strip());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}