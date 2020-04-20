package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTrashProcedure {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static String nnUri;

  @BeforeClass
  public static void beforeClass() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    nnUri = FileSystem.getDefaultUri(conf).toString();
  }

  @AfterClass
  public static void afterClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testTrashProcedure() throws Exception {
    Path src = new Path(nnUri + "/"+getMethodName()+"-src");
    Path dst = new Path(nnUri + "/"+getMethodName()+"-dst");
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(src);
    fs.mkdirs(new Path(src, "dir"));
    assertTrue(fs.exists(src));

    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    TrashProcedure trashProcedure =
        new TrashProcedure("trash-procedure", null, 1000, context);
    trashProcedure.doExecute();
    assertFalse(fs.exists(src));
  }
}
