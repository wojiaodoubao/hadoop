package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.procedure.Procedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.tools.FedBalanceConfigs.*;

public class TrashProcedure extends Procedure {

  private DistributedFileSystem srcFs;
  private FedBalanceContext context;
  private Configuration conf;

  public TrashProcedure(String name, String nextProcedure, long delayDuration,
      FedBalanceContext context) throws IOException {
    super(name, nextProcedure, delayDuration);
    this.context = context;
    this.conf = context.getConf();
    this.srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
  }

  @Override
  public boolean execute(Procedure lastProcedure) throws IOException {
    doExecute();
    return true;
  }

  void doExecute() throws IOException {
    if (srcFs.exists(context.getSrc())) {
      if (conf.getBoolean(DISTCP_PROCEDURE_MOVE_TO_TRASH,
          DISTCP_PROCEDURE_MOVE_TO_TRASH_DEFAULT)) {
        conf.setFloat(FS_TRASH_INTERVAL_KEY, 1);
        if (!Trash.moveToAppropriateTrash(srcFs, context.getSrc(), conf)) {
          throw new IOException(
              "Failed move " + context.getSrc() + " to trash.");
        }
      } else {
        if (!srcFs.delete(context.getSrc(), true)) {
          throw new IOException("Failed delete " + context.getSrc());
        }
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    context.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    context = new FedBalanceContext();
    context.readFields(in);
    conf = context.getConf();
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
  }
}
