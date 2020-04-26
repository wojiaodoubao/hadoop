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
package org.apache.hadoop.hdfs.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.procedure.BalanceProcedureConfigKeys.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.hdfs.procedure.BalanceProcedureConfigKeys.WORK_THREAD_NUM;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotSame;
import static org.mockito.ArgumentMatchers.any;

public class TestBalanceProcedureScheduler {

  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static DistributedFileSystem fs;
  private static final int DEFAULT_BLOCK_SIZE = 512;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    CONF.setInt(WORK_THREAD_NUM, 1);

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(3).build();
    cluster.waitClusterUp();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    String workPath =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/procedure";
    CONF.set(SCHEDULER_JOURNAL_URI, workPath);
    fs.mkdirs(new Path(workPath));
  }

  /**
   * Test the scheduler could be shutdown correctly.
   */
  @Test(timeout = 30000)
  public void testShutdownScheduler() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    // construct job
    BalanceJob.Builder builder = new BalanceJob.Builder<>();
    builder.nextProcedure(new WaitProcedure("wait", 1000, 30 * 1000));
    BalanceJob job = builder.build();

    scheduler.submit(job);
    Thread.sleep(1000); // wait job to be scheduled.
    scheduler.shutDownAndWait(30 * 1000);

    BalanceJournal journal =
        ReflectionUtils.newInstance(HDFSJournal.class, CONF);
    journal.clear(job);
  }

  /**
   * Test a successful job.
   */
  @Test(timeout = 30000)
  public void testSuccessfulJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // construct job
      List<RecordProcedure> procedures = new ArrayList<>();
      BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
      for (int i = 0; i < 5; i++) {
        RecordProcedure r = new RecordProcedure("record-" + i, 1000L);
        builder.nextProcedure(r);
        procedures.add(r);
      }
      BalanceJob<RecordProcedure> job = builder.build();

      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertNull(job.error());
      // verify finish list.
      assertEquals(5, RecordProcedure.getFinishList().size());
      for (int i = 0; i < RecordProcedure.getFinishList().size(); i++) {
        assertEquals(procedures.get(i), RecordProcedure.getFinishList().get(i));
      }
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test a job fails and the error can be got.
   */
  @Test
  public void testFailedJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(
          new UnrecoverableProcedure("fail", 1000, lastProcedure -> {
            throw new IOException("Job failed exception.");
          }));
      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      GenericTestUtils
          .assertExceptionContains("Job failed exception", job.error());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test recover a job.
   */
  @Test
  public void testGetJobAfterRecover() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // construct job
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(new WaitProcedure("wait", 1000, 1000))
          .removeAfterDone(false);
      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.shutDownAndWait(2);

      // restart scheduler and recover the job.
      scheduler = new BalanceProcedureScheduler(CONF);
      scheduler.init(true);
      scheduler.waitUntilDone(job);

      BalanceJob recoverJob = scheduler.findJob(job);
      assertNull(recoverJob.error());
      assertNotSame(job, recoverJob);
      assertEquals(job, recoverJob);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test RetryException is handled correctly.
   */
  @Test(timeout = 5000)
  public void testRetry() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // construct job
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      RetryProcedure retryProcedure = new RetryProcedure("retry", 1000, 3);
      builder.nextProcedure(retryProcedure);
      BalanceJob job = builder.build();

      long start = Time.now();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertNull(job.error());

      long duration = Time.now() - start;
      assertEquals(true, duration > 1000 * 3);
      assertEquals(3, retryProcedure.getTotalRetry());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test schedule an empty job.
   */
  @Test(timeout = 5000)
  public void testEmptyJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      BalanceJob job = new BalanceJob.Builder<>().build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test serialization and deserialization of Job.
   */
  @Test(timeout = 5000)
  public void testJobSerializeAndDeserialize() throws Exception {
    BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
    for (int i = 0; i < 5; i++) {
      RecordProcedure r = new RecordProcedure("record-" + i, 1000L);
      builder.nextProcedure(r);
    }
    builder.nextProcedure(new RetryProcedure("retry", 1000, 3));
    BalanceJob<RecordProcedure> job = builder.build();
    job.setId(BalanceProcedureScheduler.allocateJobId());
    // Serialize.
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    job.write(new DataOutputStream(bao));
    bao.flush();
    ByteArrayInputStream bai = new ByteArrayInputStream(bao.toByteArray());
    // Deserialize.
    BalanceJob newJob = new BalanceJob.Builder<>().build();
    newJob.readFields(new DataInputStream(bai));
    assertEquals(job, newJob);
  }

  /**
   * Test scheduler crashes and recovers.
   */
  @Test(timeout = 5000)
  public void testSchedulerDownAndRecoverJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);

    try {
      // construct job
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      MultiPhaseProcedure multiPhaseProcedure =
          new MultiPhaseProcedure("retry", 1000, 10);
      builder.nextProcedure(multiPhaseProcedure);
      BalanceJob job = builder.build();

      scheduler.submit(job);
      Thread.sleep(500); // wait procedure to be scheduled.
      scheduler.shutDownAndWait(2);

      int before = MultiPhaseProcedure.getCounter();
      MultiPhaseProcedure.setCounter(0);
      assertTrue(before > 0 && before < 10);

      // restart scheduler, test recovering the job.
      scheduler = new BalanceProcedureScheduler(CONF);
      scheduler.init(true);
      scheduler.waitUntilDone(job);
      int after = MultiPhaseProcedure.getCounter();
      assertEquals(10, after + before);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  @Test(timeout = 5000)
  public void testRecoverJobFromJournal() throws Exception {
    BalanceJournal journal =
        ReflectionUtils.newInstance(HDFSJournal.class, CONF);
    BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
    BalanceProcedure wait0 = new WaitProcedure("wait0", 1000, 5000);
    BalanceProcedure wait1 = new WaitProcedure("wait1", 1000, 1000);
    builder.nextProcedure(wait0).nextProcedure(wait1);

    BalanceJob job = builder.build();
    job.setId(BalanceProcedureScheduler.allocateJobId());
    job.setCurrentProcedure(wait1);
    job.setLastProcedure(null);
    journal.saveJob(job);

    long start = Time.now();
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      scheduler.waitUntilDone(job);
      long duration = Time.now() - start;
      assertTrue(duration >= 1000 && duration < 5000);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  @Test(timeout = 5000)
  public void testClearJournalFail() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);

    BalanceJournal journal = Mockito.mock(BalanceJournal.class);
    AtomicInteger count = new AtomicInteger(0);
    Mockito.doAnswer(invocation -> {
      if (count.incrementAndGet() == 1) {
        throw new IOException("Mock clear failure");
      }
      return null;
    }).when(journal).clear(any(BalanceJob.class));
    scheduler.setJournal(journal);

    try {
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(new WaitProcedure("wait", 1000, 1000));
      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertEquals(2, count.get());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test the job will be recovered if writing journal fails.
   */
  @Test(timeout = 30000)
  public void testJobRecoveryWhenWriteJournalFail() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);

    try {
      // construct job
      AtomicBoolean recoverFlag = new AtomicBoolean(true);
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(new WaitProcedure("wait", 1000, 1000))
          .nextProcedure(
              new UnrecoverableProcedure("shutdown", 1000, lastProcedure -> {
                cluster.restartNameNode(false);
                return true;
              })).nextProcedure(
          new UnrecoverableProcedure("recoverFlag", 1000, lastProcedure -> {
            recoverFlag.set(false);
            return true;
          })).nextProcedure(new WaitProcedure("wait", 1000, 1000));

      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertTrue(job.isJobDone());
      assertNull(job.error());
      assertTrue(recoverFlag.get());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }
}