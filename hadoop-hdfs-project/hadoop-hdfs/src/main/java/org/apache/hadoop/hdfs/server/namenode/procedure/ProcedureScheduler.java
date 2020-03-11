package org.apache.hadoop.hdfs.server.namenode.procedure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.TMP_TAIL;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM_DEFAULT;
/**
 * TODO:修改这个注释
 * The state machine framework consist of:
 *   Job:        the state machine.
 *   Task:       the state node of the state machine. it should be stateless.
 *   JobContext: the context passing through the state machine.
 *
 * Example:
 *   ArrayList<Task> tasks = new ArrayList<>();
 *   for (int i = 0; i < 5; i++) {
 *     tasks.add(new BasicTaskImpl("TASK_" + i, "TASK_" + (i + 1), 1000L));
 *   }
 *   final JobContext context = new JobContext(tasks.get(0).getName());
 *   Job job = new Job(context, tasks);
 *
 *   JobScheduler scheduler = new JobScheduler(CONF);
 *   scheduler.init();
 */
public class ProcedureScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProcedureScheduler.class);
  private ConcurrentHashMap<Job, Job> jobSet;
  private LinkedBlockingQueue<Job> runningQueue;
  private DelayQueue<DelayWrapper> delayQueue;
  private LinkedBlockingQueue<Job> recoverQueue;
  private Configuration conf;
  private Journal journal;

  private Thread reader;
  private ThreadPoolExecutor workersPool;
  private Thread rooster;
  private Thread recoverThread;
  private AtomicBoolean running = new AtomicBoolean(true);

  public ProcedureScheduler(Configuration conf) {
    this.conf = conf;
  }

  public synchronized void init() throws URISyntaxException, IOException {
    this.runningQueue = new LinkedBlockingQueue<>();
    this.delayQueue = new DelayQueue<>();
    this.recoverQueue = new LinkedBlockingQueue<>();
    this.jobSet = new ConcurrentHashMap<>();

    // start threads.
    this.rooster = new Rooster();
    this.rooster.setDaemon(true);
    rooster.start();
    this.recoverThread = new Recover();
    this.recoverThread.setDaemon(true);
    recoverThread.start();
    int workerNum = conf.getInt(WORK_THREAD_NUM, WORK_THREAD_NUM_DEFAULT);
    workersPool = new ThreadPoolExecutor(workerNum, workerNum * 2, 1,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
    this.reader = new Reader();
    this.reader.start();

    // init journal. TODO:fix this.
    journal = new HDFSJournal(conf);

    recoverAllJobs();
  }

  /**
   * Submit the job.
   */
  public synchronized void submit(Job job) throws IOException {
    if (!running.get()) {
      throw new IOException("Scheduler is shutdown.");
    }
    String jobId = allocateJobId();
    job.setId(jobId);
    job.setScheduler(this);
    journal.saveJob(job);
    jobSet.put(job, job);
    runningQueue.add(job);
    LOG.info("Add new job={}", job);
  }


  /**
   * Wait permanently until the job is done.
   */
  public void waitUntilDone(Job job) throws IOException {
    if (job.isJobDone()) {
      return;
    }
    if (!jobSet.keySet().contains(job)) {
      throw new IOException("Job has not been submitted.");
    }
    synchronized (job) {
      while (!job.isJobDone()) {
        try {
          job.wait();
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /**
   * Delay this job.
   */
  void delay(Job job, long delayInMilliseconds) {
    delayQueue.add(new DelayWrapper(job, delayInMilliseconds));
    LOG.info("Delay " + delayInMilliseconds + "ms " + job.getId());
  }

  boolean jobDone(Job job) {
    try {
      journal.clear(job);
      jobSet.remove(job);
      return true;
    } catch (IOException e) {
      LOG.warn("Failed finish job " + job.getId(), e);
      return false;
    }
  }

  /**
   * Save current status to journal.
   */
  boolean writeJournal(Job job) {
    try {
      journal.saveJob(job);
      return true;
    } catch (Exception e) {
      LOG.warn("Save procedure failed, add to recoverQueue " + job.getId(), e);
      recoverQueue.add(job);
      return false;
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  /**
   * Shutdown scheduler.
   */
  public synchronized void shutDown() {
    if (!running.get()) {
      return;
    }
    running.set(false);
    reader.interrupt();
    rooster.interrupt();
    recoverThread.interrupt();
    workersPool.shutdownNow();
    while (reader.isAlive()) {
      try {
        reader.join();
      } catch (InterruptedException e) {
      }
    }
    while (rooster.isAlive()) {
      try {
        rooster.join();
      } catch (InterruptedException e) {
      }
    }
    while (recoverThread.isAlive()) {
      try {
        recoverThread.join();
      } catch (InterruptedException e) {
      }
    }
    while (!workersPool.isTerminated()) {
      try {
        workersPool.awaitTermination(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Search all jobs and add them to recoverQueue. It's called once after the
   * scheduler starts.
   */
  private void recoverAllJobs() throws IOException {
    Job[] jobs = journal.listAllJobs();
    for (Job job : jobs) {
      recoverQueue.add(job);
      jobSet.put(job, job);
    }
  }

  private String allocateJobId() {
    return "job-" + UUID.randomUUID();
  }

  /**
   * Get all jobs.
   */
  public Set<Job> getRunningJobs() {
    if (jobSet != null) {
      return Collections.unmodifiableSet(jobSet.keySet());
    } else {
      return Collections.EMPTY_SET;
    }
  }

  // TODO: annotation to all these threads.
  class Rooster extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        try {
          DelayWrapper dJob = delayQueue.take();
          runningQueue.add(dJob.job);
          LOG.info("Wake up " + dJob.job);
        } catch (InterruptedException e) {
          // ignore interrupt exception.
        }
      }
    }
  }

  class Reader extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        try {
          final Job job = runningQueue.take();
          workersPool.submit(() -> {
            LOG.info("Start job. job=", job);
            job.execute();
            if (job.isJobDone()) {
              if (job.error() == null) {
                LOG.info("Job done. job=" + job.getId());
              } else {
                LOG.warn("Job failed. job=" + job.getId(), job.error());
              }
            } else {
              if (job.error() != null) {
                LOG.info("Will retry or recover. job=" + job.getId(),
                    job.error());
              } else {
                LOG.info("Will retry or recover. job=" + job.getId());
              }
            }
            return;
          });
        } catch (InterruptedException e) {
          // ignore interrupt exception.
        }
      }
    }
  }

  class Recover extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        Job job = null;
        try {
          job = recoverQueue.take();
        } catch (InterruptedException ie) {
          // ignore interrupt exception.
        }
        if (job != null) {
          try {
            journal.recoverJob(job);
            job.setScheduler(ProcedureScheduler.this);
            runningQueue.add(job);
            LOG.info("Recover success, add to runningQueue " + job.getId());
          } catch (IOException e) {
            LOG.warn("Recover failed, re-add to recoverQueue " + job.getId(), e);
            recoverQueue.add(job);
          }
        }
      }
    }
  }

  class DelayWrapper implements Delayed {
    Job job;
    long time;

    public DelayWrapper(Job job, long delayInMilliseconds) {
      this.job = job;
      this.time = Time.now() + delayInMilliseconds;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long delay = time - System.currentTimeMillis();
      if (delay < 0) {
        delay = 0;
      }
      return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return Ints.saturatedCast(this.time - ((DelayWrapper) o).time);
    }
  }
}
