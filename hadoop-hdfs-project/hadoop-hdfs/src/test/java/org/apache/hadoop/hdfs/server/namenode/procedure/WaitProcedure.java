package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.util.Time;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This procedure waits specified period of time then finish. It simulates the
 * behaviour of blocking procedures.
 */
public class WaitProcedure extends Procedure {

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
        if (isSchedulerShutdown()) {
          return false;
        }
      } finally {
        timeLeft = waitTime - (Time.now() - startTime);
      }
    }
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(waitTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    waitTime = in.readLong();
  }
}
