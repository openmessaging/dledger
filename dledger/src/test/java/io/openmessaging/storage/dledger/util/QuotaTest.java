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

package io.openmessaging.storage.dledger.util;

import io.openmessaging.storage.dledger.utils.Quota;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuotaTest {


    @Test
    public void testValidateNow() throws Exception {
        Quota quota = new Quota(100);
        Thread.sleep(quota.leftNow() + 1);
        for (int i = 0; i < 500; i++) {
            if (i >= 100) {
                Assertions.assertTrue(quota.validateNow());
            } else {
                Assertions.assertFalse(quota.validateNow());
            }
            quota.sample(1);
            Assertions.assertTrue(quota.leftNow() < 1000 - i);
            Assertions.assertTrue(quota.leftNow() > 1000 - i - 200);
            Thread.sleep(1);
        }
    }
}
