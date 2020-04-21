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
package org.apache.hadoop.hdfs.server.namenode.procedure;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

/**
 * A Job is a state machine consists of many procedures.
 */
public class Job<T extends Procedure> implements Writable {
  private String id;
  private ProcedureScheduler scheduler;
  private volatile boolean jobDone = false;
  private Exception error;
  public static final Logger LOG = LoggerFactory.getLogger(Job.class.getName());
  private Map<String, T> procedureTable = new HashMap<>();
  private T firstProcedure;
  private T curProcedure;
  private T lastProcedure;
  private boolean removeAfterDone = true;

  public static String NEXT_PROCEDURE_NONE = "NONE";
  static Set<String> RESERVED_NAME = new HashSet<>();

  static {
    RESERVED_NAME.add(NEXT_PROCEDURE_NONE);
  }

  public static class Builder<T extends Procedure> {

    List<T> procedures = new ArrayList<>();
    boolean removeAfterDone = false;

    public Builder addProcedure(T procedure) {
      procedures.add(procedure);
      return this;
    }

    public Builder nextProcedure(T procedure) {
      if (procedures.size() > 0) {
        procedures.get(procedures.size() - 1)
            .setNextProcedure(procedure.name());
      }
      procedure.setNextProcedure(NEXT_PROCEDURE_NONE);
      addProcedure(procedure);
      return this;
    }

    /**
     * Automatically remove this job from the scheduler cache when the job is
     * done.
     */
    public Builder removeAfterDone(boolean removeAfterDone) {
      this.removeAfterDone = removeAfterDone;
      return this;
    }

    public Job build() throws IOException {
      Job job = new Job(procedures, removeAfterDone);
      for (Procedure<T> p : procedures) {
        p.setJob(job);
      }
      return job;
    }
  }

  private Job() {}

  private Job(Iterable<T> procedures, boolean remove) throws IOException {
    for (T p : procedures) {
      String taskName = p.name();
      for (String rname : RESERVED_NAME) {
        if (rname.equals(taskName)) {
          throw new IOException(rname + " is reserved.");
        }
      }
      procedureTable.put(p.name(), p);
      if (firstProcedure == null) {
        firstProcedure = p;
      }
    }
    removeAfterDone = remove;
    lastProcedure = null;
    curProcedure = firstProcedure;
  }

  /**
   * Run the state machine.
   */
  public void execute() {
    while (!jobDone && scheduler.isRunning()) {
      if (curProcedure == null) {
        finish(null);
      } else {
        try {
          if (curProcedure == firstProcedure || lastProcedure != curProcedure) {
            LOG.info("Start procedure {}. The last procedure is {}",
                curProcedure.name(),
                lastProcedure == null ? null : lastProcedure.name());
          }
          if (curProcedure.execute(lastProcedure)) {
            lastProcedure = curProcedure;
            curProcedure = next();
          }
        } catch (Procedure.RetryException tre) {
          scheduler.delay(this, curProcedure.delayMillisBeforeRetry());
          return;
        } catch (Exception e) {
          finish(e);// This job is failed.
          return;
        } catch (Throwable t) {
          finish(new IOException("Got throwable error.", t));
          return;
        }
        if (scheduler.writeJournal(this)) {
          continue;
        } else {
          return;// write journal failed. The job is added to the recoverQueue.
        }
      }
    }
  }

  private T next() {
    if (curProcedure == null) {
      return firstProcedure;
    } else {
      return procedureTable.get(curProcedure.nextProcedure());
    }
  }

  private synchronized void finish(Exception exception) {
    assert !jobDone;
    if (scheduler.jobDone(this)) {
      jobDone = true;
      error = exception;
      notifyAll();
    }
  }

  void setScheduler(ProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }

  void setId(String id) {
    this.id = id;
  }

  /**
   * Get the uid of the job.
   */
  public String getId() {
    return this.id;
  }

  /**
   * Whether this job should be removed after it's done.
   */
  public boolean removeAfterDone() {
    return removeAfterDone;
  }

  @VisibleForTesting
  void setLastProcedure(T lastProcedure) {
    this.lastProcedure = lastProcedure;
  }

  @VisibleForTesting
  void setCurrentProcedure(T currentProcedure) {
    this.curProcedure = currentProcedure;
  }

  /**
   * Return true if the job has finished.
   */
  public boolean isJobDone() {
    return jobDone;
  }

  public synchronized void waitJobDone() throws InterruptedException {
    while (!jobDone) {
      wait();
    }
  }

  /**
   * Return the error exception during the job execution. This should be called
   * after the job finishes.
   */
  public Exception error() {
    return error;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, id);
    int taskTableSize = procedureTable == null ? 0 : procedureTable.size();
    out.writeInt(taskTableSize);
    for (T p : procedureTable.values()) {
      Text.writeString(out, p.getClass().getName());
      p.write(out);
    }
    if (firstProcedure != null) {
      Text.writeString(out, firstProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
    if (curProcedure != null) {
      Text.writeString(out, curProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
    if (lastProcedure != null) {
      Text.writeString(out, lastProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = Text.readString(in);
    procedureTable = new HashMap<>();
    int taskTableSize = in.readInt();
    for (int i = 0; i < taskTableSize; i++) {
      String className = Text.readString(in);
      try {
        T p = (T) ReflectionUtils.newInstance(Class.forName(className), null);
        p.readFields(in);
        procedureTable.put(p.name(), p);
      } catch (Exception e) {
        LOG.error("Failed reading Procedure.", e);
        throw new IOException(e);
      }
    }
    String firstProcedureName = Text.readString(in);
    if (firstProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      firstProcedure = null;
    } else {
      firstProcedure = procedureTable.get(firstProcedureName);
    }
    String currentProcedureName = Text.readString(in);
    if (currentProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      curProcedure = null;
    } else {
      curProcedure = procedureTable.get(currentProcedureName);
    }
    String lastProcedureName = Text.readString(in);
    if (lastProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      lastProcedure = null;
    } else {
      lastProcedure = procedureTable.get(currentProcedureName);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Job) {
      Job ojob = (Job) obj;
      if (!id.equals(ojob.id)) {
        return false;
      }
      if (procedureTable.size() != ojob.procedureTable.size()) {
        return false;
      }
      for (String key : procedureTable.keySet()) {
        if (!procedureTable.get(key).equals(ojob.procedureTable.get(key))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    if (firstProcedure == null) {
      return String.format("id=%s, firstProcedure=null", id);
    }
    return String.format("id=%s, firstProcedure=%s", id,
        firstProcedure.name() + "-" + firstProcedure.getClass());
  }

  boolean isSchedulerShutdown() {
    return !scheduler.isRunning();
  }
}