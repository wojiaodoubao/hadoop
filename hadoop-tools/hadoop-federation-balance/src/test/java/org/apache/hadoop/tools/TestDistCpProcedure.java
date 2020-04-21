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
package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.tools.DistCpProcedure.Stage;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.Procedure.RetryException;
import org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureScheduler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.SCHEDULER_BASE_URI;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.tools.FedBalanceConfigs.CURRENT_SNAPSHOT_NAME;
import static org.apache.hadoop.tools.FedBalanceConfigs.LAST_SNAPSHOT_NAME;
import static org.junit.Assert.*;

public class TestDistCpProcedure {
  private static MiniDFSCluster cluster;
  private static MiniMRYarnCluster mrCluster;
  private static Configuration conf;
  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  private static final long BLOCK_SIZE = 1024;
  private FileEntry[] srcfiles = {
      new FileEntry(SRCDAT, true),
      new FileEntry(SRCDAT + "/a", false),
      new FileEntry(SRCDAT + "/b", true),
      new FileEntry(SRCDAT + "/b/c", false)
  };
  private static String nnUri;

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

    nnUri = FileSystem.getDefaultUri(conf).toString();
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
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
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
    assertFalse(fs.exists(new Path(context.getSrc(), ".snapshot")));
    assertFalse(fs.exists(new Path(context.getDst(), ".snapshot")));
  }

  @Test
  public void testInitDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles, -1);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    // set permission.
    fs.setPermission(src, FsPermission.createImmutable((short) 020));

    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);

    // submit distcp.
    try {
      dcProcedure.initDistCp();
    } catch (RetryException e) {
    }
    fs.delete(new Path(src, "a"), true);
    // wait until job done.
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    assertTrue(fs.exists(dst));
    // Because we used snapshot, the file should be copied.
    assertTrue(fs.exists(new Path(dst, "a")));
  }

  @Test
  public void testDiffDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles, -1);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);

    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    assertTrue(fs.exists(dst));

    // move file out of src and test distcp.
    fs.rename(new Path(src, "a"), new Path("/a"));
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertFalse(fs.exists(new Path(dst, "a")));
    // move back file a and test distcp.
    fs.rename(new Path("/a"), new Path(src, "a"));
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertTrue(fs.exists(new Path(dst, "a")));
    // append file a and test.
    OutputStream out = fs.append(new Path(src, "a"));
    out.write("hello".getBytes());
    out.close();
    long len = fs.getFileStatus(new Path(src, "a")).getLen();
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertEquals(len, fs.getFileStatus(new Path(dst, "a")).getLen());
  }

  @Test
  public void testStageFinalDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles, -1);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    // set permission.
    fs.setPermission(src, FsPermission.createImmutable((short) 020));
    // open files.
    OutputStream out = fs.append(new Path(src, "a"));

    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    FileStatus srcStatus = fs.getFileStatus(src);
    assertEquals(0, srcStatus.getPermission().toShort());
    FileStatus dstStatus = fs.getFileStatus(dst);
    assertEquals(020, dstStatus.getPermission().toShort());
    intercept(RemoteException.class, "LeaseExpiredException",
        "Expect RemoteException(LeaseExpiredException).", () -> out.close());
  }

  @Test
  public void testStageFinish() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    fs.mkdirs(src);
    fs.mkdirs(dst);
    fs.allowSnapshot(src);
    fs.allowSnapshot(dst);
    fs.createSnapshot(src, LAST_SNAPSHOT_NAME);
    fs.createSnapshot(src, CURRENT_SNAPSHOT_NAME);
    fs.createSnapshot(dst, LAST_SNAPSHOT_NAME);

    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    dcProcedure.finish();

    assertTrue(fs.exists(dst));
    assertFalse(fs.exists(new Path(src, ".snapshot")));
    assertFalse(fs.exists(new Path(dst, ".snapshot")));
  }

  @Test
  public void testRecoveryByStage() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles, -1);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    FedBalanceContext context = new FedBalanceContext(src, dst, conf);
    final DistCpProcedure[] dcp = new DistCpProcedure[1];
    dcp[0] = new DistCpProcedure("distcp-procedure", null, 1000, context);

    dcp[0] = seDe(dcp[0]);
    executeProcedure(dcp[0], Stage.INIT_DISTCP, () -> dcp[0].preCheck());
    dcp[0] = seDe(dcp[0]);
    executeProcedure(dcp[0], Stage.DIFF_DISTCP, () -> dcp[0].initDistCp());
    fs.delete(new Path(src, "a"), true);
    dcp[0] = seDe(dcp[0]);
    executeProcedure(dcp[0], Stage.FINISH, () -> dcp[0].diffDistCp());
    dcp[0] = seDe(dcp[0]);
    OutputStream out = fs.append(new Path(src, "b/c"));
    executeProcedure(dcp[0], Stage.FINISH, () -> dcp[0].finalDistCp());
    intercept(RemoteException.class, "LeaseExpiredException",
        "Expect RemoteException(LeaseExpiredException).", () -> out.close());
    dcp[0] = seDe(dcp[0]);
    assertTrue(dcp[0].execute(null));
    assertTrue(fs.exists(dst));
    assertFalse(fs.exists(new Path(context.getSrc(), ".snapshot")));
    assertFalse(fs.exists(new Path(context.getDst(), ".snapshot")));
  }

  @Test
  public void testShutdown() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
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

    long sleep = Math.abs(new Random().nextLong()) % 10000;
    Thread.sleep(sleep);
    scheduler.shutDown();
  }

  interface Call {
    void execute() throws IOException, RetryException;
  }

  private static void executeProcedure(DistCpProcedure procedure, Stage target,
      Call call) throws IOException {
    Stage stage = Stage.PRE_CHECK;
    procedure.setStage(stage);
    while (stage != target) {
      try {
        call.execute();
      } catch (RetryException e) {
      } finally {
        stage = procedure.getStage();
      }
    }
  }

  static class FileEntry {
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

  private DistCpProcedure seDe(DistCpProcedure dcp) throws IOException {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutput dataOut = new DataOutputStream(bao);
    dcp.write(dataOut);
    dcp = new DistCpProcedure();
    dcp.readFields(
        new DataInputStream(new ByteArrayInputStream(bao.toByteArray())));
    return dcp;
  }
}
