package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.util.ArrayList;
import java.util.List;

/**
 * This procedure records all the finished procedures.
 */
public class RecordProcedure extends Procedure<RecordProcedure> {

  static List<RecordProcedure> finish = new ArrayList<>();

  public RecordProcedure() {}

  public RecordProcedure(String name, long delay) {
    super(name, delay);
  }

  @Override
  public boolean execute(RecordProcedure lastProcedure) throws RetryException {
    finish.add(this);
    return true;
  }
}
