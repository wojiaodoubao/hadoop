/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test QuotaCounts.
 * */
public class TestQuotaCounts {
  @Test
  public void testQuotaCounts() {
    QuotaCounts qc =
        new QuotaCounts.Builder().nameSpace(HdfsConstants.QUOTA_RESET)
            .storageSpace(HdfsConstants.QUOTA_RESET).build();
    assertEquals(HdfsConstants.QUOTA_RESET, qc.getNameSpace());
    assertTrue(qc.nsSsCounts == QuotaCounts.QUOTA_RESET);
    for (StorageType st : StorageType.values()) {
      assertEquals(0, qc.getTypeSpace(st));
    }
    assertTrue(qc.tsCounts == QuotaCounts.STORAGE_TYPE_DEFAULT);

    qc.setNameSpace(2);
    assertEquals(2, qc.getNameSpace());
    assertTrue(qc.nsSsCounts != QuotaCounts.QUOTA_RESET);
    qc.addNameSpace(1);

    QuotaCounts qc2 = new QuotaCounts.Builder().build();
    assertTrue(qc2.tsCounts == QuotaCounts.STORAGE_TYPE_DEFAULT);
    qc2.setTypeSpaces(qc.tsCounts);
    assertTrue(qc2.tsCounts == QuotaCounts.STORAGE_TYPE_DEFAULT);

    qc.setTypeSpace(StorageType.DISK, 100);
    assertEquals(100, qc.getTypeSpace(StorageType.DISK));
    assertTrue(qc.tsCounts != QuotaCounts.STORAGE_TYPE_DEFAULT);
    qc.addTypeSpace(StorageType.DISK, 20);

    qc.setNameSpace(0);
    qc.setStorageSpace(0);
    assertTrue(qc.nsSsCounts == QuotaCounts.QUOTA_DEFAULT);
  }
}
