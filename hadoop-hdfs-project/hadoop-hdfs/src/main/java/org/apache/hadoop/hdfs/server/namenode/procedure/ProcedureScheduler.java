package org.apache.hadoop.hdfs.server.namenode.procedure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.WORK_THREAD_NUM_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.JOURNAL_CLASS;
/**
 * The state machine framework consist of:
 *   Job:                The state machine. It implements the basic logic of the
 *                       state machine.
 *   Procedure:          The components of the job. It implements the custom
 *                       logic.
 *   ProcedureScheduler: The multi-thread model responsible for running,
 *                       recovering, handling errors and job persistence.
 *   Journal:            It handles the job persistence and recover.
 *
 * Example:
 *   Job.Builder builder = new Job.Builder<>();
 *   builder.nextProcedure(new WaitProcedure("wait", 1000, 30 * 1000));
 *   Job job = builder.build();
 *
 *   ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
 *   scheduler.init();
 *   scheduler.submit(job);
 *   scheduler.waitUntilDone(job);
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

    // init journal.
    Class<Journal> clazz =
        (Class<Journal>) conf.getClass(JOURNAL_CLASS, HDFSJournal.class);
    journal = ReflectionUtils.newInstance(clazz, conf);

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
   * Find job in scheduler.
   *
   * @return the job in scheduler. Null if the schedule has no job with the
   *         same id.
   */
  public Job findJob(Job job) {
    Job found = null;
    for (Job j : jobSet.keySet()) {
      if (j.getId().equals(job.getId())) {
        found = j;
        break;
      }
    }
    return found;
  }

  /**
   * Wait permanently until the job is done.
   */
  public void waitUntilDone(Job job) {
    Job found = findJob(job);
    if (found == null || found.isJobDone()) {
      return;
    }
    synchronized (found) {
      while (!found.isJobDone()) {
        try {
          found.wait();
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
      if (job.removeAfterDone()) {
        jobSet.remove(job);
      }
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

  /**
   * The running state of the scheduler.
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Shutdown the scheduler.
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
  }

  /**
   * Shutdown scheduler and wait at most timeout seconds for procedures to
   * finish.
   * @param timeout Wait at most timeout seconds for procedures to finish.
   */
  public synchronized void shutDownAndWait(int timeout) {
    shutDown();
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
        workersPool.awaitTermination(timeout, TimeUnit.SECONDS);
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

  @VisibleForTesting
  static String allocateJobId() {
    return "job-" + UUID.randomUUID();
  }


  /**
   * This thread consumes the delayQueue and move the jobs to the runningQueue.
   */
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

  /**
   * This thread consumes the runningQueue and give the job to the workers.
   */
  class Reader extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        try {
          final Job job = runningQueue.take();
          workersPool.submit(() -> {
            LOG.info("Start job. job=", job);
            job.execute();
            if (!running.get()) {
              return;
            }
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

  /**
   * This thread consumes the recoverQueue, recovers the job the adds it to the
   * runningQueue.
   */
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
