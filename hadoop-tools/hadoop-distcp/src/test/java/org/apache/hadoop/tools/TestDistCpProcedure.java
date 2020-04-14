package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureScheduler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.SCHEDULER_BASE_URI;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.Assert.*;

public class TestDistCpProcedure {
  private static MiniDFSCluster cluster;
  private static MiniMRYarnCluster mrCluster;
  private static Configuration conf;
  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  private static final long BLOCK_SIZE = 1024;

  @BeforeClass
  public static void beforeClass() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    mrCluster = new MiniMRYarnCluster(TestDistCpProcedure.class.getName(), 3);
    conf.set("fs.defaultFS", cluster.getFileSystem().getUri().toString());
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
    mrCluster.init(conf);
    mrCluster.start();
    conf = mrCluster.getConfig();

    String workPath =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/procedure";
    conf.set(SCHEDULER_BASE_URI, workPath);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (mrCluster != null) {
      mrCluster.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSuccessfulDistCpProcedure() throws Exception {
    String testRoot = "/user/foo/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };
    String nnUri = FileSystem.getDefaultUri(conf).toString();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles, -1);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    ProcedureScheduler scheduler = new ProcedureScheduler(conf);
    scheduler.init();

    Job balanceJob =
        new Job.Builder<>().nextProcedure(dcProcedure).build();
    scheduler.submit(balanceJob);
    scheduler.waitUntilDone(balanceJob);
    assertTrue(balanceJob.isJobDone());
    if (balanceJob.error() != null) {
      throw balanceJob.error();
    }
    assertNull(balanceJob.error());
    assertTrue(fs.exists(dst));
    assertFalse(fs.exists(src));
  }

  private class FileEntry {
    String path;
    boolean isDir;

    public FileEntry(String path, boolean isDir) {
      this.path = path;
      this.isDir = isDir;
    }

    String getPath() {
      return path;
    }

    boolean isDirectory() {
      return isDir;
    }
  }

  private void createFiles(DistributedFileSystem fs, String topdir,
      FileEntry[] entries, long chunkSize) throws IOException {
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    short replicationFactor = 2;
    for (FileEntry entry : entries) {
      Path newPath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        fs.mkdirs(newPath);
      } else {
        long fileSize = BLOCK_SIZE *100;
        int bufSize = 128;
        if (chunkSize == -1) {
          DFSTestUtil.createFile(fs, newPath, bufSize,
              fileSize, BLOCK_SIZE, replicationFactor, seed);
        } else {
          // Create a variable length block file, by creating
          // one block of half block size at the chunk boundary
          long seg1 = chunkSize * BLOCK_SIZE - BLOCK_SIZE / 2;
          long seg2 = fileSize - seg1;
          DFSTestUtil.createFile(fs, newPath, bufSize,
              seg1, BLOCK_SIZE, replicationFactor, seed);
          DFSTestUtil.appendFileNewBlock(fs, newPath, (int)seg2);
        }
      }
      seed = System.currentTimeMillis() + rand.nextLong();
    }
  }
}
