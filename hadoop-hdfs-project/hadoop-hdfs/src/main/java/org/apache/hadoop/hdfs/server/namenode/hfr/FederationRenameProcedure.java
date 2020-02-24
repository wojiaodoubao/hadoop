package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FRHelper;
import org.apache.hadoop.hdfs.server.namenode.procedure.BasicTaskImpl;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.JobContext;
import org.apache.hadoop.hdfs.server.namenode.procedure.JobScheduler;
import org.apache.hadoop.hdfs.server.namenode.procedure.Task;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Time;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_LINK_THREAD_NUM;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_LINK_THREAD_NUM_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_WORK_PATH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_WORK_PATH_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.procedure.Constants.SCHEDULER_BASE_URI;

public class FederationRenameProcedure {

  public static final Log LOG =
      LogFactory.getLog(FederationRenameProcedure.class);
  public static final String FEDERATION_RENAME_PROCEDURE_MV_TRASH =
      "federation.rename.procedure.mv.trash";
  public static final boolean FEDERATION_RENAME_PROCEDURE_MV_TRASH_DEFAULT =
      true;
  public static final String FEDERATION_RENAME_SAVE_RPC_TIMEOUT =
      "federation.rename.save.rpc.timeout";
  public static final int FEDERATION_RENAME_SAVE_RPC_TIMEOUT_DEFAULT =
      10 * 60000;// 10min
  public static final String FEDERATION_RENAME_GRAFT_RPC_TIMEOUT =
      "federation.rename.graft.rpc.timeout";
  public static final int FEDERATION_RENAME_GRAFT_RPC_TIMEOUT_DEFAULT =
      10 * 60000;// 10min
  public static final String FEDERATION_RENAME_CHECK_THREAD =
      "federation.rename.check.thread.num";
  public static final String FEDERATION_RENAME_CHECK_TYPE =
      "federation.rename.check.type";
  public static final String FEDERATION_RENAME_CHECK_TYPE_DEFAULT = "location";
  public static final int FEDERATION_RENAME_CHECK_THREAD_DEFAULT = 20;
  protected Configuration conf;

  public FederationRenameProcedure(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Start a FederationRenameProcedure from shell command. This method will
   * start a JobScheduler and submit a FederationRename Job to it. After the job
   * finishing, the scheduler will be shut down.
   *
   * Submit a new federation rename job. It also recovers the unfinished jobs.
   *   - [ submit ] src_uri dst_uri persist_cluster caller key value ...
   *
   * Recover the unfinished jobs.
   *   - [continue] key value key value ...
   */
  public static void main(String args[])
      throws IOException, URISyntaxException, InterruptedException {
    new FederationRenameProcedure(new Configuration()).execute(args);
  }

  public void execute(String args[])
      throws IOException, URISyntaxException, InterruptedException {
    int index = 0;
    String cmd = args[index++];
    if (cmd.equals("submit")) {
      submit(args, index);
    } else if (cmd.equals("continue")) {
      continueJob(args, index);
    }
  }

  protected void submit(String args[], int index)
      throws IOException, URISyntaxException, InterruptedException {
    // parse input arguments.
    URI src = new URI(args[index++]);
    URI dst = new URI(args[index++]);
    URI frCluster = new URI(args[index++]);
    String caller = args[index++];
    while (args.length > index) {
      if (args[index].equals("--scheduler_work_path_uri")) {
        index++;
        String schedulerWorkpath = args[index++];
        conf.set(SCHEDULER_BASE_URI, schedulerWorkpath);
      } else {
        conf.set(args[index++], args[index++]);
      }
    }

    // construct BasicInfo.
    String globalID =
        InetAddress.getLocalHost().getHostName() + "-" + Time.now();
    String workBase = conf.get(FEDERATION_RENAME_WORK_PATH,
        FEDERATION_RENAME_WORK_PATH_DEFAULT);
    Path workPath = new Path(workBase, globalID);
    URI contextURI =
        FederationRenameJobContext.getURI(frCluster, workPath.toString());
    BasicInfo info = new BasicInfo.Builder().setSrc(src).setDst(dst)
        .setCaller(caller).setStartTime(Time.now()).setContextURI(contextURI)
        .setGlobalID(globalID).setSafeSrc(src).setSafeDst(dst)
        .setSrcPoolId("").setDstPoolId("").build();

    Job job = createJob(conf, info);
    JobScheduler scheduler = new JobScheduler();
    scheduler.init(conf);
    scheduler.schedule(job);

    Set<Job> jobs = scheduler.getRunningJobs();
    while (jobs.size() > 0) {
      for (Job rJob : jobs) {
        rJob.waitJobDone();
      }
      jobs = scheduler.getRunningJobs();
    }
    scheduler.shutDown();
  }

  void continueJob(String args[], int index)
      throws IOException, URISyntaxException, InterruptedException {
    Configuration conf = new Configuration();
    while (args.length > index) {
      if (args[index].equals("--scheduler_work_path_uri")) {
        index++;
        String schedulerWorkpath = args[index++];
        conf.set(SCHEDULER_BASE_URI, schedulerWorkpath);
      } else {
        conf.set(args[index++], args[index++]);
      }
    }

    JobScheduler scheduler = new JobScheduler();
    scheduler.init(conf);

    Set<Job> jobs = scheduler.getRunningJobs();
    while (jobs.size() > 0) {
      Thread.sleep(1000);
      jobs = scheduler.getRunningJobs();
    }
    scheduler.shutDown();
  }

  protected Job createJob(Configuration conf, BasicInfo info) throws IOException {
    ArrayList<Task> tasks = new ArrayList<>();
    tasks.add(new Prepare("PREPARE", "SAVE_SUB_TREE", 10 * 1000L));
    tasks.add(new SaveSubTree("SAVE_SUB_TREE", "GRAFT_SUB_TREE", 1000L));
    tasks.add(new GraftSubTree("GRAFT_SUB_TREE", "HARD_LINK", 1000L));
    tasks.add(new HardLink("HARD_LINK", "FINISH", 1000L));
    tasks.add(new Finish("FINISH", "NONE", 1000L));
    Job job =
        new Job(new FederationRenameJobContext("PREPARE", info, conf), tasks);
    return job;
  }

  public static class PermissionStatus implements Writable {

    private AclStatus acl;
    private FsPermission permission;

    public PermissionStatus setAcl(AclStatus acl) {
      this.acl = acl;
      return this;
    }

    public PermissionStatus setFsPermission(FsPermission permission) {
      this.permission = permission;
      return this;
    }

    public FsPermission getPermission() {
      return permission;
    }

    public AclStatus getAcl() {
      return acl;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (permission == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        permission.write(out);
      }
      if (acl == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PBHelperClient.convert(acl).writeDelimitedTo(bout);
        byte[] data = bout.toByteArray();
        out.writeInt(data.length);
        out.write(data);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      if (in.readBoolean()) {
        permission = FsPermission.read(in);
      }
      if (in.readBoolean()) {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        AclProtos.GetAclStatusResponseProto proto =
            AclProtos.GetAclStatusResponseProto.parseDelimitedFrom(bin);
        acl = PBHelperClient.convert(proto);
      }
    }
  }

  public static class FederationRenameJobContext extends JobContext {
    public BasicInfo info;
    public Configuration conf;
    public PermissionStatus pStatus;

    public FederationRenameJobContext() {
    }

    public FederationRenameJobContext(String firstTask, BasicInfo info,
        Configuration conf) throws IOException {
      super(firstTask);
      this.info = info;
      this.conf = conf;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      info = new BasicInfo.Builder().build();
      info.readFields(in);
      conf = new Configuration(false);
      conf.readFields(in);
      if (in.readBoolean()) {
        pStatus = new PermissionStatus();
        pStatus.readFields(in);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      info.write(out);
      conf.write(out);
      if (pStatus == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        pStatus.write(out);
      }
    }

    public static URI getURI(URI base, String child) throws URISyntaxException {
      String res = base.toString();
      if (child.startsWith("/")) {
        child = child.substring(1);
      }
      if (res.endsWith("/")) {
        return new URI(res + child);
      } else {
        return new URI(res + "/" + child);
      }
    }
  }

  public static class Prepare<T extends FederationRenameJobContext> extends BasicTaskImpl<T> {
    public Prepare() {}

    public Prepare(String name, String nextTask, long delay) {
      super(name, nextTask, delay);
    }

    @Override
    public T execute(T context) throws TaskRetryException {
      try {
        BasicInfo info = context.info;
        Configuration conf = context.conf;
        final DistributedFileSystem sFs =
            (DistributedFileSystem) FileSystem.get(info.src, conf);
        final DistributedFileSystem dFs =
            (DistributedFileSystem) FileSystem.get(info.dst, conf);

        // build basic info.
        FederationRenameUtils.buildSafeSrcAndDstPath(context, false, true);
        // create work path.
        String workBase = conf.get(FEDERATION_RENAME_WORK_PATH,
            FEDERATION_RENAME_WORK_PATH_DEFAULT);
        Path workPath = new Path(workBase, info.globalID);
        if (dFs.exists(workPath)) {
          // For idempotent, move the workPath to trash.
          Trash.moveToAppropriateTrash(dFs, workPath, context.conf);
        }
        dFs.mkdirs(workPath);
        // sanity check.
        FederationRenameUtils.sanityCheck(context);
        // set pool ids.
        FederationRenameUtils.buildPool(context);
        // cancel & save permission of src.
        if (context.pStatus != null && context.pStatus.permission != null) {
          // The permission has already been stored. We must skip so we won't
          // override the stored permission with (short)000.
        } else {
          FederationRenameUtils.cancelAndSaveSrcExecutePermission(context);
        }
        // cancel safeDst's parent x permission.
        dFs.setPermission(new Path(info.safeDst).getParent(),
            FsPermission.createImmutable((short) 000));
        // save basic info.
        FederationRenameUtils.saveBasicInfo(context);

        if (!FederationRenameUtils.closeOpenFiles(sFs, info.getSrcPath(),
            conf.getInt(FEDERATION_RENAME_LINK_THREAD_NUM,
                FEDERATION_RENAME_LINK_THREAD_NUM_DEFAULT))) {
          LOG.info("Failed close all open files, will retry !");
          throw new TaskRetryException();
        }
        return super.execute(context);
      } catch (Exception e) {
        FederationRenameProcedure.LOG.warn("Failed doing " + this.getName(), e);
        throw new TaskRetryException(e);
      }
    }
  }

  public static class SaveSubTree<T extends FederationRenameJobContext>
      extends BasicTaskImpl<T> {
    public SaveSubTree() {}

    public SaveSubTree(String name, String nextTask, long delay) {
      super(name, nextTask, delay);
    }

    @Override
    public T execute(T context) throws TaskRetryException {
      try {
        Configuration saveConf = new Configuration(context.conf);
        saveConf.setBoolean(IPC_CLIENT_PING_KEY, false);
        saveConf.setInt(IPC_PING_INTERVAL_KEY,
            context.conf.getInt(FEDERATION_RENAME_SAVE_RPC_TIMEOUT,
                FEDERATION_RENAME_SAVE_RPC_TIMEOUT_DEFAULT));
        DistributedFileSystem sFs = (DistributedFileSystem) FileSystem
            .get(context.info.src, saveConf);
        DistributedFileSystem pFs = (DistributedFileSystem) FileSystem
            .get(context.info.contextURI, saveConf);
        Path treeFile = FRHelper
            .getFederationRenameTreePath(context.info.contextURI.getPath());
        Path strTable = FRHelper.getFederationRenameStringTablePath(
            context.info.contextURI.getPath());
        Path treeMeta = FRHelper
            .getFederationRenameTreeMetaPath(context.info.contextURI.getPath());

        // If treeFile exists then it's a redo. Delete it to keep idempotent.
        if (pFs.exists(treeFile)) {
          pFs.delete(treeFile, false);
        }
        if (pFs.exists(treeMeta)) {
          pFs.delete(treeMeta, false);
        }
        if (pFs.exists(strTable)) {
          pFs.delete(strTable, false);
        }

        if (sFs.saveSubTree(context.info.contextURI.toString())) {
          return super.execute(context);
        } else {
          throw new IOException("saveSubTree failed.");
        }
      } catch (Exception e) {
        FederationRenameProcedure.LOG.warn("Failed doing " + this.getName(), e);
        throw new TaskRetryException();
      }
    }
  }

  public static class GraftSubTree<T extends FederationRenameJobContext>
      extends BasicTaskImpl<T> {
    public GraftSubTree() {}

    public GraftSubTree(String name, String nextTask, long delay) {
      super(name, nextTask, delay);
    }

    @Override
    public T execute(T context) throws TaskRetryException {
      try {
        Configuration graftConf = new Configuration(context.conf);
        graftConf.setBoolean(IPC_CLIENT_PING_KEY, false);
        graftConf.setInt(IPC_PING_INTERVAL_KEY,
            context.conf.getInt(FEDERATION_RENAME_GRAFT_RPC_TIMEOUT,
                FEDERATION_RENAME_GRAFT_RPC_TIMEOUT_DEFAULT));
        DistributedFileSystem dFs = (DistributedFileSystem) FileSystem
            .get(context.info.dst, graftConf);

        if (dFs.exists(new Path(context.info.getSafeDstPath()))) {
          return super.execute(context);
        }
        if (dFs.graftSubTree(context.info.contextURI.toString())) {
          return super.execute(context);
        } else {
          throw new IOException("graftSubTree failed.");
        }
      } catch (Exception e) {
        FederationRenameProcedure.LOG.warn("Failed doing " + this.getName(), e);
        throw new TaskRetryException();
      }
    }
  }

  public static class HardLink<T extends FederationRenameJobContext>
      extends BasicTaskImpl<T> {
    public HardLink() {}

    public HardLink(String name, String nextTask, long delay) {
      super(name, nextTask, delay);
    }

    @Override
    public T execute(T context) throws TaskRetryException {
      try {
        BlockLinker linker =
            new BlockLinker(context.info.contextURI, context.conf);
        linker.linkReplicas();
        return super.execute(context);
      } catch (Exception e) {
        FederationRenameProcedure.LOG.warn("Failed doing " + this.getName(), e);
        throw new TaskRetryException();
      }
    }
  }

  public static class Finish<T extends FederationRenameJobContext>
      extends BasicTaskImpl<T> {
    public Finish() {}

    public Finish(String name, String nextTask, long delay) {
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

        // clean src path.
        if (sFs.exists(new Path(context.info.getSrcPath()))) {
          if (context.conf.getBoolean(FEDERATION_RENAME_PROCEDURE_MV_TRASH,
              FEDERATION_RENAME_PROCEDURE_MV_TRASH_DEFAULT)) {
            Trash.moveToAppropriateTrash(sFs,
                new Path(context.info.getSrcPath()), context.conf);
            LOG.info("Move to trash " + context.info.getSrcPath());
          } else {
            // TODO: Remove the double check after FedV2 runs stably for a while.
            LOG.info("Type y to delete " + new Path(context.info.getSrcPath()));
            Scanner sc = new Scanner(System.in);
            String command = sc.nextLine();
            if (command.equalsIgnoreCase("y")) {
              sFs.delete(new Path(context.info.getSrcPath()), true);
              LOG.info("Deleted " + context.info.getSrcPath());
            } else {
              LOG.info("Skip delete " + context.info.getSrcPath());
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

