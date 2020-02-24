package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.procedure.BasicTaskImpl;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.Task;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Start a ZkMountTableProcedure from shell command. This method will
 * start a JobScheduler and submit a ZkMountTableProcedure Job to it. After the
 * job finishing, the scheduler will be shut down.
 * ZkMountTableProcedure will update mount table on zk before deleting src.
 *
 * Submit a new federation rename job. It also recovers the unfinished jobs.
 *   - [ submit ] src_uri dst_uri persist_cluster caller key value ...
 *
 * Recover the unfinished jobs.
 *   - [continue] key value key value ...
 */
public class ZkMountTableProcedure extends FederationRenameProcedure {
  public static final Log LOG = LogFactory.getLog(ZkMountTableProcedure.class);
  public static final String FEDERATION_RENAME_PROCEDURE_MOUNT_CLUSTER =
      "federation.rename.procedure.mount.cluster";

  public ZkMountTableProcedure(Configuration conf) {
    super(conf);
  }

  public static void main(String args[])
      throws IOException, URISyntaxException, InterruptedException {
    new ZkMountTableProcedure(new Configuration()).execute(args);
  }

  @Override
  protected Job createJob(Configuration conf, BasicInfo info)
      throws IOException {
    ArrayList<Task> tasks = new ArrayList<>();
    tasks.add(new Prepare("PREPARE", "SAVE_SUB_TREE", 10 * 1000L));
    tasks.add(new SaveSubTree("SAVE_SUB_TREE", "GRAFT_SUB_TREE", 1000L));
    tasks.add(new GraftSubTree("GRAFT_SUB_TREE", "HARD_LINK", 1000L));
    tasks.add(new HardLink("HARD_LINK", "ZkMountTableFinish", 1000L));
    tasks.add(new ZkMountTableFinish("ZkMountTableFinish", "NONE", 1000L));
    Job job =
        new Job(new FederationRenameJobContext("PREPARE", info, conf), tasks);
    return job;
  }

  static class ZkMountTableFinish<T extends FederationRenameJobContext>
      extends BasicTaskImpl<T> {

    public ZkMountTableFinish() {
    }

    public ZkMountTableFinish(String name, String nextTask, long delay) {
      super(name, nextTask, delay);
    }

    @Override
    public T execute(T context) throws TaskRetryException {
      try {
        DistributedFileSystem sFs = (DistributedFileSystem) FileSystem
            .get(context.info.src, context.conf);
        DistributedFileSystem dFs = (DistributedFileSystem) FileSystem
            .get(context.info.dst, context.conf);
        Path safeDstPath = new Path(context.info.getSafeDstPath());
        Path dstPath = new Path(context.info.getDstPath());

        // restore permission & rename dst path.
        if (dFs.exists(safeDstPath)) {
          dFs.removeAcl(safeDstPath);
          dFs.modifyAclEntries(safeDstPath,
              context.pStatus.getAcl().getEntries());
          dFs.setPermission(safeDstPath, context.pStatus.getPermission());
          if (!dFs.exists(dstPath)) {
            dFs.rename(safeDstPath, dstPath, Options.Rename.NONE);
          } else {
            throw new IOException(dstPath + " already exists !");
          }
        }

        // check same directories.
        if (sFs.exists(new Path(context.info.getSrcPath()))) {
          FederationRenameUtils.checkSameDirectories(context.conf, sFs, dFs,
              new Path(context.info.getSrcPath()),
              new Path(context.info.getDstPath()));
        }

        // clean safeDstPath's parent.
        if (dFs.exists(safeDstPath.getParent())) {
          if (!dFs.delete(safeDstPath.getParent(), false)) {
            throw new IOException(
                "Failed deleting safe dst parent " + safeDstPath);
          }
        }

        // update mount table.
        FederationRenameUtils.updateMountTable(context);

        // clean children of src. Leave src as a place holder in case some
        // client's mount tables are not updated in time.
        Path srcPath = new Path(context.info.getSrcPath());
        if (sFs.exists(srcPath)) {
          boolean mvToTrash = context.conf
              .getBoolean(FEDERATION_RENAME_PROCEDURE_MV_TRASH,
                  FEDERATION_RENAME_PROCEDURE_MV_TRASH_DEFAULT);
          FileStatus[] children = sFs.listStatus(srcPath);
          for (FileStatus child : children) {
            if (mvToTrash) {
              Trash.moveToAppropriateTrash(sFs, child.getPath(), context.conf);
              LOG.info("Move to trash " + child.getPath());
            } else {
              // TODO: Remove the double check after FedV2 runs stably for a while.
              LOG.info("Type y to delete " + child.getPath());
              Scanner sc = new Scanner(System.in);
              String command = sc.nextLine();
              if (command.equalsIgnoreCase("y")) {
                sFs.delete(child.getPath(), true);
                LOG.info("Deleted " + child.getPath());
              } else {
                LOG.info("Skip delete " + child.getPath());
              }
            }
          }
        }

        return super.execute(context);
      } catch (Exception e) {
        FederationRenameProcedure.LOG.warn("Failed doing " + this.getName(), e);
        throw new TaskRetryException();
      }
    }
  }
}