package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.io.IOException;

/**
 * UnrecoverableProcedure will lose the handler if it's recovered.
 */
public class UnrecoverableProcedure extends Procedure {

  public interface Call {
    boolean execute(Procedure lastProcedure) throws RetryException, IOException;
  }

  private Call handler;

  public UnrecoverableProcedure() {}

  /**
   * The handler will be lost if the procedure is recovered.
   */
  public UnrecoverableProcedure(String name, long delay, Call handler) {
    super(name, delay);
    this.handler = handler;
  }

  @Override
  public boolean execute(Procedure lastProcedure) throws RetryException,
      IOException {
    if (handler != null) {
      return handler.execute(lastProcedure);
    } else {
      return true;
    }
  }
}
