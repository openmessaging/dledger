/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DLedgerJsonUtilsTest {

    private static final byte[] BODY = new byte[] {0, 1, (byte) 0xFF, 65};

    @Test
    public void testParseFastjson1Base64Fixture() {
        AppendEntryRequest request = DLedgerJsonUtils.parseObject(
            "{\"body\":\"AAH/QQ==\"}", AppendEntryRequest.class);

        Assertions.assertArrayEquals(BODY, request.getBody());
    }

    @Test
    public void testSerializeAppendEntryRequestBodyAsBase64() {
        AppendEntryRequest request = new AppendEntryRequest();
        request.setBody(BODY);

        String json = DLedgerJsonUtils.toJsonString(request);

        Assertions.assertTrue(json.contains("\"body\":\"AAH/QQ==\""), json);
        Assertions.assertFalse(json.contains("\"body\":["), json);
    }

    @Test
    public void testRoundTripBatchAppendEntryRequestBodiesAsBase64() {
        BatchAppendEntryRequest request = new BatchAppendEntryRequest();
        request.setBatchMsgs(Arrays.asList(BODY, new byte[] {2, 3}));

        String json = DLedgerJsonUtils.toJsonString(request);
        BatchAppendEntryRequest decoded = DLedgerJsonUtils.parseObject(json, BatchAppendEntryRequest.class);

        Assertions.assertTrue(json.contains("\"batchMsgs\":[\"AAH/QQ==\",\"AgM=\"]"), json);
        Assertions.assertEquals(2, decoded.getBatchMsgs().size());
        Assertions.assertArrayEquals(BODY, decoded.getBatchMsgs().get(0));
        Assertions.assertArrayEquals(new byte[] {2, 3}, decoded.getBatchMsgs().get(1));
    }

    @Test
    public void testRoundTripPushEntryRequestBodyWithByteArrayApi() {
        DLedgerEntry entry = new DLedgerEntry();
        entry.setBody(BODY);
        PushEntryRequest request = new PushEntryRequest();
        request.setEntry(entry);

        byte[] jsonBytes = DLedgerJsonUtils.toJsonBytes(request);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        PushEntryRequest decoded = DLedgerJsonUtils.parseObject(jsonBytes, PushEntryRequest.class);

        Assertions.assertTrue(json.contains("\"body\":\"AAH/QQ==\""), json);
        Assertions.assertFalse(json.contains("\"body\":["), json);
        Assertions.assertArrayEquals(BODY, decoded.getEntry().getBody());
    }

    @Test
    public void testRoundTripSnapshotMetaFixture() {
        SnapshotMeta snapshotMeta = DLedgerJsonUtils.parseObject(
            "{\"lastIncludedIndex\":10,\"lastIncludedTerm\":2}", SnapshotMeta.class);

        Assertions.assertEquals(10, snapshotMeta.getLastIncludedIndex());
        Assertions.assertEquals(2, snapshotMeta.getLastIncludedTerm());

        String json = DLedgerJsonUtils.toJsonString(snapshotMeta);
        SnapshotMeta decoded = DLedgerJsonUtils.parseObject(json, SnapshotMeta.class);

        Assertions.assertEquals(10, decoded.getLastIncludedIndex());
        Assertions.assertEquals(2, decoded.getLastIncludedTerm());
    }
}
