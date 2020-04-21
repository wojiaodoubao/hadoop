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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.hdfs.server.namenode.procedure.Job.NEXT_PROCEDURE_NONE;

/**
 * The components of the Job. It implements the custom logic.
 */
public abstract class Procedure<T extends Procedure> implements Writable {

  public static final Logger LOG =
      LoggerFactory.getLogger(Procedure.class.getName());
  private String nextProcedure;
  private String name;
  private long delayDuration;
  private Job job;

  public Procedure() {
  }

  public Procedure(String name, String nextProcedure, long delayDuration) {
    this();
    this.name = name;
    this.nextProcedure = nextProcedure;
    this.delayDuration = delayDuration;
  }

  public Procedure(String name, long delayDuration) {
    this(name, NEXT_PROCEDURE_NONE, delayDuration);
  }

  /**
   * The main process. This is called by the ProcedureScheduler.

   * Make sure the process quits fast when it's interrupted and the scheduler is
   * shut down.
   *
   * @param lastProcedure the last procedure.
   * @throws RetryException if this procedure needs delay a while then retry.
   * @return true if the procedure has done and the job will go to the next
   *         procedure, otherwise false.
   */
  public abstract boolean execute(T lastProcedure)
      throws RetryException, IOException;

  /**
   * The time in milliseconds the procedure should wait before retry.
   */
  public long delayMillisBeforeRetry() {
    return delayDuration;
  }

  /**
   * The active flag.
   */
  protected boolean isSchedulerShutdown() {
    return job.isSchedulerShutdown();
  }

  protected void setNextProcedure(String nextProcedure) {
    this.nextProcedure = nextProcedure;
  }

  void setJob(Job job) {
    this.job = job;
  }

  public String nextProcedure() {
    return nextProcedure;
  }

  public String name() {
    return name;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (nextProcedure == null) {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    } else {
      Text.writeString(out, nextProcedure);
    }
    Text.writeString(out, name);
    new LongWritable(delayDuration).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    nextProcedure = Text.readString(in);
    name = Text.readString(in);
    delayDuration = readLong(in);
  }

  private static long readLong(DataInput in) throws IOException {
    LongWritable delayWritable = new LongWritable();
    delayWritable.readFields(in);
    return delayWritable.get();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Procedure) {
      Procedure p = (Procedure) obj;
      return isEquals(nextProcedure, p.nextProcedure) && isEquals(name, p.name)
          && delayDuration == p.delayDuration;
    }
    return false;
  }

  private boolean isEquals(Object a, Object b) {
    if (a == null) {
      return b == null;
    } else {
      return a.equals(b);
    }
  }

  /**
   * The RetryException represents the current procedure should be delayed then
   * retried.
   */
  public static class RetryException extends Exception {
    public RetryException() {
    }

    public RetryException(String msg) {
      super(msg);
    }

    public RetryException(Exception e) {
      super(e);
    }
  }
}
