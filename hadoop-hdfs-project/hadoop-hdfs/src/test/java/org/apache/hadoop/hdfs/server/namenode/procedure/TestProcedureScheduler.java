package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.SCHEDULER_BASE_URI;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM;
import static org.junit.Assert.*;

public class TestProcedureScheduler {

  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static DistributedFileSystem fs;

  @BeforeClass
  public static void setup() throws IOException, URISyntaxException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    final int DEFAULT_BLOCK_SIZE = 512;
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    CONF.setInt(WORK_THREAD_NUM, 1);

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(3).build();
    cluster.waitClusterUp();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    String workPath =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/procedure";
    CONF.set(SCHEDULER_BASE_URI, workPath);
    fs.mkdirs(new Path(workPath));
  }

  @Test(timeout = 30000)
  public void testShutdownScheduler()
      throws IOException, URISyntaxException, InterruptedException {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
    scheduler.init();
    // construct job
    Job.Builder builder = new Job.Builder<>();
    builder.nextProcedure(new WaitProcedure("WAIT", 1000, 30 * 1000));
    Job job = builder.build();

    scheduler.submit(job);
    Thread.sleep(1000);// wait job to be scheduled.
    scheduler.shutDownAndWait(2);
    scheduler.waitUntilDone(job);
    GenericTestUtils
        .assertExceptionContains("Scheduler is shutdown", job.error());
  }

  @Test(timeout = 30000)
  public void testSuccessfulJob() throws Exception {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
    scheduler.init();
    try {
      // construct job
      List<RecordProcedure> procedures = new ArrayList<>();
      Job.Builder builder = new Job.Builder<RecordProcedure>();
      for (int i = 0; i < 5; i++) {
        RecordProcedure r = new RecordProcedure("RECORD_" + i, 1000L);
        builder.nextProcedure(r);
        procedures.add(r);
      }
      Job<RecordProcedure> job = builder.build();

      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertNull(job.error());
      // verify finish list.
      assertEquals(5, RecordProcedure.finish.size());
      for (int i = 0; i < RecordProcedure.finish.size(); i++) {
        assertEquals(procedures.get(i), RecordProcedure.finish.get(i));
      }
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  @Test
  public void testRetry() throws Exception {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
    scheduler.init();
    try {
      // construct job
      Job.Builder builder = new Job.Builder<>();
      RetryProcedure retryProcedure = new RetryProcedure("retry", 1000, 3);
      builder.nextProcedure(retryProcedure);
      Job job = builder.build();

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

  @Test
  public void testEmptyJob() throws Exception {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
    scheduler.init();
    Job job = new Job.Builder<>().build();
    scheduler.submit(job);
    scheduler.waitUntilDone(job);
  }

  @Test
  public void testJobSerializeAndDeserialize() throws Exception {
    Job.Builder builder = new Job.Builder<RecordProcedure>();
    for (int i = 0; i < 5; i++) {
      RecordProcedure r = new RecordProcedure("record-" + i, 1000L);
      builder.nextProcedure(r);
    }
    builder.nextProcedure(new RetryProcedure("retry", 1000, 3));
    Job<RecordProcedure> job = builder.build();
    job.setId(ProcedureScheduler.allocateJobId());
    // Serialize.
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    job.write(new DataOutputStream(bao));
    bao.flush();
    ByteArrayInputStream bai = new ByteArrayInputStream(bao.toByteArray());
    // Deserialize.
    Job newJob = new Job.Builder<>().build();
    newJob.readFields(new DataInputStream(bai));
    assertEquals(job, newJob);
  }

  @Test
  public void testSchedulerDownAndRecoverJob() throws Exception {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
    scheduler.init();

    // construct job
    Job.Builder builder = new Job.Builder<>();
    MultiPhaseProcedure multiPhaseProcedure =
        new MultiPhaseProcedure("retry", 1000, 10);
    builder.addProcedure(multiPhaseProcedure);
    Job job = builder.build();

    scheduler.submit(job);
    Thread.sleep(500);// wait procedure to be scheduled.
    scheduler.shutDownAndWait(2);

    int before = MultiPhaseProcedure.getCounter();
    MultiPhaseProcedure.setCounter(0);
    assertTrue(before > 0 && before < 10);

    // restart scheduler, test recovering the job.
    scheduler = new ProcedureScheduler(CONF);
    scheduler.init();
    scheduler.waitUntilDone(job);
    int after = MultiPhaseProcedure.getCounter();
    assertEquals(10, after + before);
  }
}
//
//  @Test
//  public void testHdfsDownAndRecoverJob() throws Exception {
//    final AtomicBoolean restart = new AtomicBoolean(false);
//    class InnerJobScheduler extends JobScheduler {
//
//      public InnerJobScheduler() throws IOException {
//      }
//
//      @Override
//      boolean saveContext(Job job) {
//        if (restart.get()) {
//          try {
//            restart.set(false);
//            cluster.restartNameNode(false);
//          } catch (IOException e) {
//          }
//        }
//        return super.saveContext(job);
//      }
//    }
//    InnerJobScheduler scheduler = new InnerJobScheduler();
//    scheduler.init(CONF);
//    ArrayList<Task> tasks = new ArrayList<>();
//    tasks.add(new RecordTask("TASK_0", "RETRY_TASK", 1000L));
//    tasks.add(new RetryTask("RETRY_TASK", "TASK_1", 1000L, 5));
//    tasks.add(new RecordTask("TASK_1", "DONE", 1000L));
//    RecordContext jcontext = new RecordContext(tasks.get(0).getName());
//    Job job = new Job(jcontext, tasks);
//
//    scheduler.schedule(job);
//    Thread.sleep(1000);
//    restart.set(true);
//    cluster.waitActive();
//    job.waitJobDone();
//
//    jcontext = (RecordContext)job.getContext();
//    List<Task> finished = jcontext.getFinishTasks();
//    assertEquals(tasks.size(), finished.size());
//    for (int i = 0; i < tasks.size(); i++) {
//      assertEquals(tasks.get(i), finished.get(i));
//    }
//  }
//
//  @Test
//  public void testSaveContext() throws Exception {
//    JobScheduler scheduler = new JobScheduler();
//    scheduler.init(CONF);
//    LabelContext label = new LabelContext();
//    label.label = "A";
//    Job job = new Job(label, new ArrayList<Task>());
//    job.setId("foo-id");
//    assertTrue(scheduler.saveContext(job));
//    label.label = "B";
//    assertTrue(scheduler.saveContext(job));
//  }
//
//  @Test
//  public void testGetLatestContext() throws Exception {
//    JobScheduler scheduler = new JobScheduler();
//    scheduler.init(CONF);
//    Job job = new Job(new JobContext(), new ArrayList<Task>());
//    job.setId("foo-id");
//
//    Path workPath = new Path(CONF.get(SCHEDULER_BASE_URI));
//    Path jobPath = new Path(workPath, "foo-id");
//    fs.mkdirs(jobPath);
//    OutputStream out = null;
//    out = fs.create(new Path(jobPath, CONTEXT_PREFIX + "1500"));
//    out.close();
//    out = fs.create(new Path(jobPath, CONTEXT_PREFIX + "1501"));
//    out.close();
//
//    assertEquals(new Path(jobPath, CONTEXT_PREFIX + "1501"),
//        scheduler.getLatestContextLog(job));
//  }
//}
//TODO 加一个单测，测试当一个job完成了之后，再recover这个job，这个job其实没有执行任何procedure。
//TODO 加一个单测，测试writeJournal失败后shutdown。
class MultiPhaseProcedure extends Procedure {

  private int totalPhase;
  private int currentPhase = 0;
  static int counter = 0;

  public MultiPhaseProcedure() {}

  public MultiPhaseProcedure(String name, long delay, int totalPhase) {
    super(name, delay);
    this.totalPhase = totalPhase;
  }

  @Override
  public boolean execute(Procedure lastProcedure)
      throws RetryException, IOException {
    if (currentPhase < totalPhase) {
      LOG.info("phase " + currentPhase);
      currentPhase++;
      counter++;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      return false;
    }
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(totalPhase);
    out.writeInt(currentPhase);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    totalPhase = in.readInt();
    currentPhase = in.readInt();
  }

  public static int getCounter() {
    return counter;
  }

  public static void setCounter(int counter) {
    MultiPhaseProcedure.counter = counter;
  }
}

class RetryProcedure extends Procedure {

  private int retryTime = 1;
  private int totalRetry = 0;

  public RetryProcedure() {}

  public RetryProcedure(String name, long delay, int retryTime) {
    super(name, delay);
    this.retryTime = retryTime;
  }

  @Override
  public boolean execute(Procedure lastProcedure) throws RetryException {
    if (retryTime > 0) {
      retryTime--;
      totalRetry++;
      throw new RetryException();
    }
    return true;
  }

  public int getTotalRetry() {
    return totalRetry;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(retryTime);
    out.writeInt(totalRetry);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    retryTime = in.readInt();
    totalRetry = in.readInt();
  }
}

/**
 * This procedure records all the finished procedures.
 */
class RecordProcedure extends Procedure<RecordProcedure> {

  static List<RecordProcedure> finish = new ArrayList<>();

  public RecordProcedure() {}

  public RecordProcedure(String name, long delay) {
    super(name, delay);
  }

  @Override
  public boolean execute(RecordProcedure lastProcedure) throws RetryException {
    finish.add(this);
    return true;
  }
}

/**
 * This procedure waits specified period of time then finish. It simulates the
 * behaviour of blocking procedures.
 */
class WaitProcedure extends Procedure {

  long waitTime;

  public WaitProcedure() {
  }

  public WaitProcedure(String name, long delay, long waitTime) {
    super(name, delay);
    this.waitTime = waitTime;
  }

  @Override
  public boolean execute(Procedure lastProcedure) throws IOException {
    long startTime = Time.now();
    long timeLeft = waitTime;
    while (timeLeft > 0) {
      try {
        Thread.sleep(timeLeft);
      } catch (InterruptedException e) {
        verifySchedulerShutdown();
        timeLeft = Time.now() - startTime;
      }
    }
    return true;
  }
}
