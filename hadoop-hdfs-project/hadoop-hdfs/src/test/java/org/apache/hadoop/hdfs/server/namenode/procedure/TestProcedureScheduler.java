package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.server.namenode.procedure.Job.NEXT_PROCEDURE_NONE;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.SCHEDULER_BASE_URI;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void testStartStopJobScheduler()
      throws IOException, URISyntaxException, InterruptedException {
    ProcedureScheduler scheduler = new ProcedureScheduler(CONF);

    // construct job
    Job.Builder builder = new Job.Builder<>();
    builder.nextProcedure(new WaitProcedure("WAIT_TASK", 1000L, 30 * 1000));
    Job job = builder.build();

    scheduler.submit(job);
    Thread.sleep(1000);// wait job to be scheduled.
    scheduler.shutDown();
  }

//  @Test
//  public void testRunAJob() throws Exception {
//    JobScheduler scheduler = new JobScheduler();
//    scheduler.init(CONF);
//    try {
//      // construct job
//      ArrayList<Task> tasks = new ArrayList<>();
//      for (int i = 0; i < 5; i++) {
//        tasks.add(new RecordTask("TASK_" + i, "TASK_" + (i + 1), 1000L));
//      }
//      final RecordContext jcontext = new RecordContext(tasks.get(0).getName());
//      Job job = new Job(jcontext, tasks);
//
//      scheduler.schedule(job);
//      while (!job.isJobDone()) {
//        try {
//          job.waitJobDone();
//        } catch (InterruptedException e) {
//        }
//      }
//
//      List<Task> finished = jcontext.getFinishTasks();
//      assertEquals(tasks.size(), finished.size());
//      for (int i = 0; i < tasks.size(); i++) {
//        assertEquals(tasks.get(i), finished.get(i));
//      }
//    } finally {
//      scheduler.shutDown();
//    }
//  }
//
//  @Test
//  public void testRetry() throws Exception {
//    JobScheduler scheduler = new JobScheduler();
//    scheduler.init(CONF);
//    try {
//      RetryTask retryTask =
//          new RetryTask("RETRY_TASK", "DONE", 1000L, 3);
//      ArrayList<Task> tasks = new ArrayList<>();
//      tasks.add(retryTask);
//      final RecordContext jcontext = new RecordContext(tasks.get(0).getName());
//      Job job = new Job(jcontext, tasks);
//      long start = Time.now();
//      scheduler.schedule(job);
//      job.waitJobDone();
//      long duration = Time.now() - start;
//
//      List<Task> finished = jcontext.getFinishTasks();
//      assertEquals(tasks.size(), finished.size());
//      for (int i = 0; i < tasks.size(); i++) {
//        assertEquals(tasks.get(i), finished.get(i));
//      }
//      assertEquals(true, duration > 1000 * 3);
//      assertEquals(3, retryTask.getTotalRetry());
//    } finally {
//      scheduler.shutDown();
//    }
//  }
//
//  @Test
//  public void testJobSeDe() throws Exception {
//    ArrayList<Task> tasks = new ArrayList<>();
//    for (int i = 0; i < 5; i++) {
//      tasks.add(new BasicTaskImpl("TASK_" + i, "TASK_" + (i + 1), 1000L));
//    }
//    final RecordContext jcontext = new RecordContext(tasks.get(0).getName());
//    RecordContext.class.getConstructors();
//    Job job = new Job(jcontext, tasks);
//
//    job.setId("newId");
//    ByteArrayOutputStream bao = new ByteArrayOutputStream();
//    job.write(new DataOutputStream(bao));
//    bao.flush();
//    ByteArrayInputStream bai = new ByteArrayInputStream(bao.toByteArray());
//    Job newJob = new Job();
//    newJob.readFields(new DataInputStream(bai));
//
//    assertEquals(job, newJob);
//    assertEquals(job.getContext(), newJob.getContext());
//  }
//
//  @Test
//  public void testSchedulerDownAndRecoverJob() throws Exception {
//    // prepare
//    JobScheduler scheduler = new JobScheduler();
//    scheduler.init(CONF);
//    ArrayList<Task> tasks = new ArrayList<>();
//    tasks.add(new RecordTask("TASK_FIRST", "RETRY_TASK", 1000L));
//    RetryTask retryTask = new RetryTask("RETRY_TASK", "TASK_0", 1000L, 5);
//    tasks.add(retryTask);
//    for (int i = 0; i < 5; i++) {
//      tasks.add(new RecordTask("TASK_" + i, "TASK_" + (i + 1), 1000L));
//    }
//    RecordContext jcontext = new RecordContext(tasks.get(0).getName());
//    Job job = new Job(jcontext, tasks);
//
//    // set FORCE_RETRY=true to make the job retrying.
//    RetryTask.FORCE_RETRY = true;
//    scheduler.schedule(job);
//    Thread.sleep(1000);
//    scheduler.shutDown();
//
//    // restart scheduler, test recovering the job.
//    scheduler.init(CONF);
//    Set<Job> jobs = null;
//    while (jobs == null || jobs.size() == 0) {
//      jobs = scheduler.getRunningJobs();
//      Thread.sleep(1000);
//    }
//    assertEquals(1, jobs.size());
//
//    RetryTask.FORCE_RETRY = false;
//    job = jobs.iterator().next();
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
//class RecordTask extends BasicTaskImpl<RecordContext> {
//
//  public RecordTask() {};
//
//  public RecordTask(String name, String nextTask, long delay) {
//    super(name, nextTask, delay);
//  }
//
//  @Override
//  public RecordContext execute(RecordContext context)
//      throws TaskRetryException {
//    context = super.execute(context);
//    context.taskDone(this);
//    return context;
//  }
//}
//class RetryTask extends BasicTaskImpl<RecordContext> {
//
//  public static volatile boolean FORCE_RETRY = false;
//  private int retryTime = 1;
//  private int totalRetry = 0;
//
//  public RetryTask() {}
//
//  public RetryTask(String name, String nextTask, long delay,
//      int retryTime) {
//    super(name, nextTask, delay);
//    this.retryTime = retryTime;
//  }
//
//  @Override
//  public RecordContext execute(RecordContext context)
//      throws TaskRetryException {
//    if (retryTime > 0) {
//      retryTime--;
//      totalRetry++;
//      throw new TaskRetryException();
//    } else if (FORCE_RETRY) {
//      totalRetry++;
//      throw new TaskRetryException();
//    }
//    context.setNextTask(nextTask);
//    context.taskDone(this);
//    return context;
//  }
//
//  public int getTotalRetry() {
//    return totalRetry;
//  }
}

/**
 * This procedure waits specified period of time then finish.
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
  public void execute(Procedure lastProcedure) throws RetryException {
    long startTime = Time.now();
    long timeLeft = waitTime;
    while (timeLeft > 0) {
      try {
        Thread.sleep(timeLeft);
      } catch (InterruptedException e) {
        if (isActive()) {
          timeLeft = Time.now() - startTime;
        } else {
          return;
        }
      }
    }
  }
}
