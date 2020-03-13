package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RetryProcedure extends Procedure {

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
