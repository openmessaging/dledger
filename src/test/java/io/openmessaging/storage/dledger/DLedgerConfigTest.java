/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

public class DLedgerConfigTest {

    @Test
    public void testParseArguments() {
        DLedgerConfig dLedgerConfig = new DLedgerConfig();
        JCommander.Builder builder = JCommander.newBuilder().addObject(dLedgerConfig);
        JCommander jc = builder.build();
        jc.parse("-i", "n1", "-g", "test", "-p", "n1-localhost:21911", "-s", "/tmp");
        Assert.assertEquals("n1", dLedgerConfig.getSelfId());
        Assert.assertEquals("test", dLedgerConfig.getGroup());
        Assert.assertEquals("n1-localhost:21911", dLedgerConfig.getPeers());
        Assert.assertEquals("/tmp", dLedgerConfig.getStoreBaseDir());
    }
}
