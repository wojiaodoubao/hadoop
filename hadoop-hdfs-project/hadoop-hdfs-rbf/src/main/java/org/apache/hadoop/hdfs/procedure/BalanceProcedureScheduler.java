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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
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

import static org.apache.hadoop.hdfs.procedure.BalanceProcedureConfigKeys.WORK_THREAD_NUM;
import static org.apache.hadoop.hdfs.procedure.BalanceProcedureConfigKeys.WORK_THREAD_NUM_DEFAULT;
import static org.apache.hadoop.hdfs.procedure.BalanceProcedureConfigKeys.JOURNAL_CLASS;
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
public class BalanceProcedureScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(BalanceProcedureScheduler.class);
  private ConcurrentHashMap<BalanceJob, BalanceJob> jobSet;
  private LinkedBlockingQueue<BalanceJob> runningQueue;
  private DelayQueue<DelayWrapper> delayQueue;
  private LinkedBlockingQueue<BalanceJob> recoverQueue;
  private Configuration conf;
  private BalanceJournal journal;

  private Thread reader;
  private ThreadPoolExecutor workersPool;
  private Thread rooster;
  private Thread recoverThread;
  private AtomicBoolean running = new AtomicBoolean(true);

  public BalanceProcedureScheduler(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Init the scheduler.
   *
   * @param recoverJobs whether to recover all the jobs from journal or not.
   */
  public synchronized void init(boolean recoverJobs) throws IOException {
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
    Class<BalanceJournal> clazz =
        (Class<BalanceJournal>) conf.getClass(JOURNAL_CLASS, HDFSJournal.class);
    journal = ReflectionUtils.newInstance(clazz, conf);

    if (recoverJobs) {
      recoverAllJobs();
    }
  }

  /**
   * Submit the job.
   */
  public synchronized void submit(BalanceJob job) throws IOException {
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
   * Remove the job from scheduler if it finishes.
   */
  public BalanceJob remove(BalanceJob job) {
    BalanceJob inner = findJob(job);
    if (inner == null) {
      return null;
    } else if (job.isJobDone()) {
      synchronized (this) {
        return jobSet.remove(inner);
      }
    }
    return null;
  }

  /**
   * Find job in scheduler.
   *
   * @return the job in scheduler. Null if the schedule has no job with the
   *         same id.
   */
  public BalanceJob findJob(BalanceJob job) {
    BalanceJob found = null;
    for (BalanceJob j : jobSet.keySet()) {
      if (j.getId().equals(job.getId())) {
        found = j;
        break;
      }
    }
    return found;
  }

  /**
   * Return all jobs in the scheduler.
   */
  public Collection<BalanceJob> getAllJobs() {
    return jobSet.values();
  }

  /**
   * Wait permanently until the job is done.
   */
  public void waitUntilDone(BalanceJob job) {
    BalanceJob found = findJob(job);
    if (found == null || found.isJobDone()) {
      return;
    }
    while (!found.isJobDone()) {
      try {
        found.waitJobDone();
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Delay this job.
   */
  void delay(BalanceJob job, long delayInMilliseconds) {
    delayQueue.add(new DelayWrapper(job, delayInMilliseconds));
    LOG.info("Need delay {}ms. Add to delayQueue. job={}", delayInMilliseconds,
        job.getId());
  }

  boolean jobDone(BalanceJob job) {
    try {
      journal.clear(job);
      if (job.removeAfterDone()) {
        jobSet.remove(job);
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Clear journal failed, add to recoverQueue. job=" + job.getId(),
          e);
      recoverQueue.add(job);
      return false;
    }
  }

  /**
   * Save current status to journal.
   */
  boolean writeJournal(BalanceJob job) {
    try {
      journal.saveJob(job);
      return true;
    } catch (Exception e) {
      LOG.warn("Save procedure failed, add to recoverQueue. job=" + job.getId(),
          e);
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
    BalanceJob[] jobs = journal.listAllJobs();
    for (BalanceJob job : jobs) {
      recoverQueue.add(job);
      jobSet.put(job, job);
    }
  }

  @VisibleForTesting
  static String allocateJobId() {
    return "job-" + UUID.randomUUID();
  }

  @VisibleForTesting
  public void setJournal(BalanceJournal journal) {
    this.journal = journal;
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
          runningQueue.add(dJob.getJob());
          LOG.info("Wake up job={}", dJob.getJob());
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
          final BalanceJob job = runningQueue.take();
          workersPool.submit(() -> {
            LOG.info("Start job. job={}", job);
            job.execute();
            if (!running.get()) {
              return;
            }
            if (job.isJobDone()) {
              if (job.error() == null) {
                LOG.info("Job done. job={}", job.getId());
              } else {
                LOG.warn("Job failed. job=" + job.getId(), job.error());
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
        BalanceJob job = null;
        try {
          job = recoverQueue.take();
        } catch (InterruptedException ie) {
          // ignore interrupt exception.
        }
        if (job != null) {
          try {
            journal.recoverJob(job);
            job.setScheduler(BalanceProcedureScheduler.this);
            runningQueue.add(job);
            LOG.info("Recover success, add to runningQueue. job={}",
                job.getId());
          } catch (IOException e) {
            LOG.warn("Recover failed, re-add to recoverQueue. job="
                + job.getId(), e);
            recoverQueue.add(job);
          }
        }
      }
    }
  }

  /**
   * Wrap the delayed BalanceJob.
   */
  private class DelayWrapper implements Delayed {
    private BalanceJob job;
    private long time;

    DelayWrapper(BalanceJob job, long delayInMilliseconds) {
      this.job = job;
      this.time = Time.now() + delayInMilliseconds;
    }

    BalanceJob getJob() {
      return job;
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

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DelayWrapper) {
        return compareTo((Delayed) obj) == 0;
      }
      return false;
    }
  }
}
