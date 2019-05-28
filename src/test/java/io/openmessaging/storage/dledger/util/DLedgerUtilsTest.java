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

package io.openmessaging.storage.dledger.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class DLedgerUtilsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();
  
  @PrepareForTest({DLedgerUtils.class, System.class})
  @Test
  public void testComputNextTimeMillis() {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1515585600000L);

    assertEquals(1515628800000L, DLedgerUtils.computNextMorningTimeMillis());
    assertEquals(1515585660000L, DLedgerUtils.computNextMinutesTimeMillis());
    assertEquals(1515589200000L, DLedgerUtils.computNextHourTimeMillis());
    assertEquals(1515591000000L, DLedgerUtils.computNextHalfHourTimeMillis());
  }

  @Test
  public void testCrc32() {
    assertEquals(0, DLedgerUtils.crc32(null));
    assertEquals(0, DLedgerUtils.crc32(new byte[]{}));
    assertEquals(417155788, DLedgerUtils.crc32(new byte[]{100}));
  }

  @Test
  public void testBytes2string() {
    assertEquals("98", DLedgerUtils.bytes2string(new byte[]{(byte)0x98}));
    assertEquals("9866", 
      DLedgerUtils.bytes2string(new byte[]{(byte)0x98, (byte)0x66}));
  }

  @Test
  public void testString2bytes() {
    assertNull(DLedgerUtils.string2bytes(""));
    assertArrayEquals(new byte[] {-1}, DLedgerUtils.string2bytes(")@"));
    assertArrayEquals(new byte[] {42}, DLedgerUtils.string2bytes("2a"));
  }

  @Test
  public void testUncompress() throws IOException {
    final byte[] src = {120, 1, 1, 3, 0, -4, -1, 0, 0, 0, 0, 3, 0, 1};
    assertArrayEquals(new byte[] {0, 0, 0}, DLedgerUtils.uncompress(src));
  }

  @Test
  public void testCompress() throws IOException {
    assertArrayEquals(
        new byte[] {120, 1, 1, 3, 0, -4, -1, 0, 0, 0, 0, 3, 0, 1},
        DLedgerUtils.compress(new byte[]{0, 0, 0}, 0)
      );
  }

  @Test
  public void testFrontStringAtLeast() {
    assertNull(DLedgerUtils.frontStringAtLeast(null, 1));

    assertEquals("a\'b\'c", DLedgerUtils.frontStringAtLeast("a\'b\'c", 6));
    assertEquals("a", DLedgerUtils.frontStringAtLeast("a\'b\'c", 1));
    
    thrown.expect(StringIndexOutOfBoundsException.class);
    DLedgerUtils.frontStringAtLeast("a\'b\'c", -1);
    // Method is not expected to return due to exception thrown
  }

  @Test
  public void testIsBlank() {
     assertFalse(DLedgerUtils.isBlank("3"));
     assertTrue(DLedgerUtils.isBlank(" "));
     assertTrue(DLedgerUtils.isBlank(""));
     assertTrue(DLedgerUtils.isBlank(null));
  }

  @Test
  public void testIsInternalIP() {
    assertFalse(DLedgerUtils.isInternalIP(new byte[]{0, 0, 0, 0}));
    assertTrue(DLedgerUtils.isInternalIP(new byte[]{10, 80, 0, 0}));
    assertTrue(DLedgerUtils.isInternalIP(new byte[]{-84, 24, 0, 0}));
    assertFalse(DLedgerUtils.isInternalIP(new byte[]{-84, 80, 0, 0}));
    assertFalse(DLedgerUtils.isInternalIP(new byte[]{-84, 0, 0, 0}));
    assertTrue(DLedgerUtils.isInternalIP(new byte[]{-64, -88, 0, 0}));
    assertFalse(DLedgerUtils.isInternalIP(new byte[]{-64, 80, 0, 0}));

    thrown.expect(RuntimeException.class);
    DLedgerUtils.isInternalIP(new byte[]{-64, -88, 0, 0, 0});
    // Method is not expected to return due to exception thrown
  }

  @Test
  public void testIpToIPv4Str() {
    assertNull(DLedgerUtils.ipToIPv4Str(new byte[]{}));
    assertEquals("8.0.65.8", DLedgerUtils.ipToIPv4Str(new byte[]{8, 0, 65, 8}));
  }
}
