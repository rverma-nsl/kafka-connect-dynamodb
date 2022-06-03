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

    private static final String TXN_DYNAMO = "{\"payload\":\"H4sIAAAAAAAAAAXBx6JrQAAA0A+yYMQEi7dwo0UfPXZEH73L179zCu0O8Qp2+iegWuY6xsK7Kyv4tq6v9gisTRwYCFBwJlM6IxdTjTBpb7XOb7Kz389AaOh5ZNmczrtEHfvpVh6auY7hzdZVivV0OXg117K4qpTsNct11P3lflzA0+53JPwlaIa652WSEw7UzXvVLusQa8bgS883X1XuDDDegHuGTVWbMCJ0cuC4RelsgMqIbhUlIzwH7N+MzaJLhZyxWKY3JYaBUcr0p4Thz9p/qy82NOeYQkGBrDwDXSxY/7q24PEAV4ta9JykqPm7NsElrHWhCmVM7itNpmJhclOXcS8yi0+ldxF4iu5oA27qZHYdUj3b6cVK+vyY76jhYv55wP1pKw0voOJgx12x1jUrqOKgxeAAWj7wmcK/4gNFHw6ioO7NpNkQfVi3I9omZg/qWS55aVxfCxbvtmJAC76dzLkbzfcBy1ykVPZxs1KpE9oOz+YacMJSfTUjkjrqjOBH6IT4ttXMH3I3W/DXG4g4C3hchZgYkmbSZzxdhL9WBcIeqSSQqPMF6g0gHYPmD6tbDLFiVXluP3nRa9E2ChASAlU+SPX97z97VgfkMAIAAA==\",\"transId\":\"730159305639\",\"containerCuName\":\"Management of Logistics\",\"id\":\"astestm302_730159305639\",\"userId\":959926885238,\"status\":\"TRIGGERED\"}";
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
        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, TXN);
        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(transformedRecord.value(), TXN_DYNAMO.strip());
    }
}