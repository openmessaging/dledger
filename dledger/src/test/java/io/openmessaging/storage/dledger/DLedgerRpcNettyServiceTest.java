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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DLedgerRpcNettyServiceTest extends ServerTestHarness {

    @Test
    public void testRemotingCodecColdStart() throws Exception {
        runColdStartProbe();
    }

    private void runColdStartProbe() throws Exception {
        String javaExecutable = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable, "-cp", classPath, ColdStartProbe.class.getName());
        processBuilder.environment().remove("JAVA_TOOL_OPTIONS");
        processBuilder.environment().remove("_JAVA_OPTIONS");
        processBuilder.environment().remove("JDK_JAVA_OPTIONS");
        Process process = processBuilder.redirectErrorStream(true).start();

        boolean finished = process.waitFor(10, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            Assertions.fail("Cold-start probe did not finish");
        }
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }
        Assertions.assertEquals(0, process.exitValue(), output.toString());
    }

    @Test
    public void testAppendAndGetBinaryBodyOverNetwork() {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:" + nextPort();
        DLedgerServer server = launchServer(group, peers, "n0", "n0", DLedgerConfig.MEMORY);
        DLedgerClient client = launchClient(group, peers);
        byte[] body = new byte[] {0, 1, (byte) 0xFF, 65};
        try {
            AppendEntryResponse appendResponse = client.append(body);
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendResponse.getCode());

            GetEntriesResponse getResponse = client.get(appendResponse.getIndex());
            Assertions.assertEquals(1, getResponse.getEntries().size());
            Assertions.assertArrayEquals(body, getResponse.getEntries().get(0).getBody());
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    public void testAppendCompletesWhenConnectionFails() throws Exception {
        AbstractDLedgerServer server = Mockito.mock(AbstractDLedgerServer.class);
        Mockito.when(server.getListenAddress()).thenReturn("localhost:" + nextPort());
        Mockito.when(server.getPeerAddr(Mockito.anyString(), Mockito.anyString()))
            .thenReturn("localhost:1");

        DLedgerRpcNettyService rpcService = new DLedgerRpcNettyService(server);
        rpcService.startup();
        try {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup("group");
            request.setRemoteId("n0");
            request.setBody(new byte[] {1});

            AppendEntryResponse response = rpcService.append(request).get(5, TimeUnit.SECONDS);

            Assertions.assertEquals(DLedgerResponseCode.NETWORK_ERROR.getCode(), response.getCode());
        } finally {
            rpcService.shutdown();
        }
    }

    public static final class ColdStartProbe {

        private ColdStartProbe() {
        }

        public static void main(String[] args) {
            try {
                ConsumerConnection connection = new ConsumerConnection();
                String json = RemotingSerializable.toJson(connection, false);
                ConsumerConnection decoded = RemotingSerializable.fromJson(json, ConsumerConnection.class);
                if (decoded == null || decoded.getConnectionSet() == null) {
                    throw new AssertionError("Remoting codec returned an incomplete ConsumerConnection: " + json);
                }
                Runtime.getRuntime().halt(0);
            } catch (Throwable t) {
                t.printStackTrace(System.err);
                System.err.flush();
                Runtime.getRuntime().halt(1);
            }
        }
    }

}
