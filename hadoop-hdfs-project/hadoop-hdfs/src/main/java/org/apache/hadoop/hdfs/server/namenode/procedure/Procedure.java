package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.server.namenode.procedure.Job.NEXT_PROCEDURE_NONE;

public abstract class Procedure implements Writable {

  public static final Logger LOG =
      LoggerFactory.getLogger(Procedure.class.getName());
  private String nextProcedure;
  private String name;
  private long delayDuration;
  private AtomicBoolean active;

  Procedure() {
    active = new AtomicBoolean(true);
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

   * Make sure the process quits when it's interrupted and the active flag is
   * unset. That means the job or the scheduler is shutting down. The process
   * should quit as soon as possible.
   *
   * @param lastProcedure the last procedure.
   * @throws RetryException if this procedure needs retry.
   * @return the name of the next task.
   */
  public abstract void execute(Procedure lastProcedure) throws RetryException;

  /**
   * The time in milliseconds the procedure should wait before retry.
   */
  public long delayMillisBeforeRetry() {
    return delayDuration;
  }

  /**
   * Deactivate the procedure. Unset the active flag.
   */
  void deactivate() {
    active.set(false);
  }

  /**
   * The active flag.
   */
  protected boolean isActive() {
    return active.get();
  }

  protected void jobDone() {
    setNextProcedure(NEXT_PROCEDURE_NONE);
  }

  protected void setNextProcedure(String nextProcedure) {
    this.nextProcedure = nextProcedure;
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

  public static class RetryException extends Exception {
    public RetryException() {}
    public RetryException(Exception e) {
      super(e);
    }
  }
}
