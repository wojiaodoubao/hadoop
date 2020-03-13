package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MultiPhaseProcedure extends Procedure {

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
