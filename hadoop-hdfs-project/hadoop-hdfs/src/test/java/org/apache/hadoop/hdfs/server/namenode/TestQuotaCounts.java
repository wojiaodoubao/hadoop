package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    QuotaCounts qc2 = new QuotaCounts.Builder().build();
    assertTrue(qc2.tsCounts == QuotaCounts.STORAGE_TYPE_DEFAULT);
    qc2.setTypeSpaces(qc.tsCounts);
    assertTrue(qc2.tsCounts != QuotaCounts.STORAGE_TYPE_DEFAULT);

    qc.setTypeSpace(StorageType.DISK, 100);
    assertEquals(100, qc.getTypeSpace(StorageType.DISK));
    assertTrue(qc.tsCounts != QuotaCounts.STORAGE_TYPE_DEFAULT);

    qc.setNameSpace(0);
    qc.setStorageSpace(0);
    assertTrue(qc.nsSsCounts == QuotaCounts.QUOTA_DEFAULT);
  }
}
