package org.apache.hadoop.tools;

import org.apache.hadoop.hdfs.server.namenode.procedure.Procedure;

import java.io.IOException;

public class DistCpFedBalance {

  static class DistCpProcedure extends Procedure {

//    public DistCpProcedure() {}

    @Override
    public boolean execute(Procedure lastProcedure)
        throws RetryException, IOException {
      return false;
    }
  }

  static class
}
