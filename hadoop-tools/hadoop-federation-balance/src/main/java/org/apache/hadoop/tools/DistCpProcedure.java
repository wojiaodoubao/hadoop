/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.procedure.BalanceProcedure;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.tools.FedBalanceConfigs.DISTCP_PROCEDURE_MAP_NUM;
import static org.apache.hadoop.tools.FedBalanceConfigs.DISTCP_PROCEDURE_MAP_NUM_DEFAULT;
import static org.apache.hadoop.tools.FedBalanceConfigs.DISTCP_PROCEDURE_BAND_WIDTH_LIMIT;
import static org.apache.hadoop.tools.FedBalanceConfigs.DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT;
import static org.apache.hadoop.tools.FedBalanceConfigs.CURRENT_SNAPSHOT_NAME;
import static org.apache.hadoop.tools.FedBalanceConfigs.LAST_SNAPSHOT_NAME;

/**
 * Copy data through distcp. Super user privilege needed.
 *
 * PRE_CHECK               :pre-check of src and dst.
 * INIT_DISTCP             :the first round of distcp.
 * DIFF_DISTCP             :copy snapshot diff round by round until there is
 *                          no diff.
 * DISABLE_WRITE           :disable write operations.
 * FINAL_DISTCP            :close all open files and do the final round distcp.
 * FINISH                  :procedure finish.
 */
public class DistCpProcedure extends BalanceProcedure {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpProcedure.class);

  /* Stages of this procedure. */
  enum Stage {
    PRE_CHECK, INIT_DISTCP, DIFF_DISTCP, DISABLE_WRITE, FINAL_DISTCP, FINISH
  }

  private FedBalanceContext context; // the balance context.
  private Path src; // the source path including the source cluster.
  private Path dst; // the dst path including the dst cluster.
  private Configuration conf;
  private int mapNum; // the number of map tasks.
  private int bandWidth; // the bandwidth limit of each distcp task.
  private String jobId; // the id of the current distcp.
  private Stage stage; // current stage of this procedure.

  /* Force close all open files when there is no diff between src and dst */
  private boolean forceCloseOpenFiles;
  /* Disable write by setting the mount point readonly. */
  private boolean useMountReadOnly;

  private FsPermission fPerm; // the permission of the src.
  private AclStatus acl; // the acl of the src.

  private JobClient client;
  private DistributedFileSystem srcFs; // fs of the src cluster.
  private DistributedFileSystem dstFs; // fs of the dst cluster.

  public DistCpProcedure() {
  }

  /**
   * The constructor of DistCpProcedure.
   *
   * @param name the name of the procedure.
   * @param nextProcedure the name of the next procedure.
   * @param delayDuration the delay duration when this procedure is delayed.
   * @param context the federation balance context.
   */
  public DistCpProcedure(String name, String nextProcedure, long delayDuration,
      FedBalanceContext context) throws IOException {
    super(name, nextProcedure, delayDuration);
    this.context = context;
    this.src = context.getSrc();
    this.dst = context.getDst();
    this.conf = context.getConf();
    this.client = new JobClient(conf);
    this.stage = Stage.PRE_CHECK;
    this.mapNum =
        conf.getInt(DISTCP_PROCEDURE_MAP_NUM, DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
    this.bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
        DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
    this.forceCloseOpenFiles = context.getForceCloseOpenFiles();
    this.useMountReadOnly = context.getUseMountReadOnly();
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
    dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
  }

  @Override
  public boolean execute() throws RetryException, IOException {
    LOG.info("Stage={}", stage.name());
    switch (stage) {
    case PRE_CHECK:
      preCheck();
      return false;
    case INIT_DISTCP:
      initDistCp();
      return false;
    case DIFF_DISTCP:
      diffDistCp();
      return false;
    case DISABLE_WRITE:
      disableWrite();
      return false;
    case FINAL_DISTCP:
      finalDistCp();
      return false;
    case FINISH:
      finish();
      return true;
    default:
      throw new IOException("Unexpected stage=" + stage);
    }
  }

  /**
   * Pre check of src and dst.
   */
  void preCheck() throws IOException {
    FileStatus status = srcFs.getFileStatus(src);
    if (!status.isDirectory()) {
      throw new IOException(src + " doesn't exist.");
    }
    if (dstFs.exists(dst)) {
      throw new IOException(dst + " already exists.");
    }
    if (srcFs.exists(new Path(src, ".snapshot"))) {
      throw new IOException(src + " shouldn't enable snapshot.");
    }
    stage = Stage.INIT_DISTCP;
  }

  /**
   * The initial distcp. Copying src to dst.
   */
  void initDistCp() throws IOException, RetryException {
    RunningJob job = getCurrentJob();
    if (job != null) {
      // the distcp has been submitted.
      if (job.isComplete()) {
        jobId = null; // unset jobId because the job is done.
        if (job.isSuccessful()) {
          stage = Stage.DIFF_DISTCP;
          return;
        } else {
          LOG.warn("DistCp failed. Failure={}", job.getFailureInfo());
        }
      } else {
        throw new RetryException();
      }
    } else {
      cleanUpBeforeInitDistcp();
      srcFs.createSnapshot(src, CURRENT_SNAPSHOT_NAME);
      jobId = submitDistCpJob(
          src.toString() + "/.snapshot/" + CURRENT_SNAPSHOT_NAME,
          dst.toString(), false);
    }
  }

  /**
   * The distcp copying diffs between LAST_SNAPSHOT_NAME and
   * CURRENT_SNAPSHOT_NAME.
   */
  void diffDistCp() throws IOException, RetryException {
    RunningJob job = getCurrentJob();
    if (job != null) {
      if (job.isComplete()) {
        jobId = null;
        if (job.isSuccessful()) {
          LOG.info("DistCp succeeded. jobId={}", job.getID().toString());
        } else {
          throw new IOException("DistCp failed. jobId=" + job.getID().toString()
              + " failure=" + job.getFailureInfo());
        }
      } else {
        throw new RetryException(); // wait job complete.
      }
    } else if (!verifyDiff()) {
      if (!verifyOpenFiles() || forceCloseOpenFiles) {
        stage = Stage.DISABLE_WRITE;
      } else {
        throw new RetryException();
      }
    } else {
      submitDiffDistCp();
    }
  }

  /**
   * Disable write either by making the mount entry readonly or cancelling the x
   * permission of the source path.
   */
  void disableWrite() throws IOException {
    if (useMountReadOnly) {
      String mount = context.getMount();
      MountTableProcedure.disableWrite(mount, conf);
    } else {
      // Save and cancel permission.
      FileStatus status = srcFs.getFileStatus(src);
      fPerm = status.getPermission();
      acl = srcFs.getAclStatus(src);
      srcFs.setPermission(src, FsPermission.createImmutable((short) 0));
    }
    stage = Stage.FINAL_DISTCP;
  }

  /**
   * Enable write by restoring the x permission.
   */
  void restorePermission() throws IOException {
    // restore permission.
    dstFs.removeAcl(dst);
    if (acl != null) {
      dstFs.modifyAclEntries(dst, acl.getEntries());
    }
    if (fPerm != null) {
      dstFs.setPermission(dst, fPerm);
    }
  }

  /**
   * Close all open files then submit the distcp with -diff.
   */
  void finalDistCp() throws IOException, RetryException {
    // Close all open files then do the final distcp.
    closeAllOpenFiles(srcFs, src);
    // Final distcp.
    RunningJob job = getCurrentJob();
    if (job != null) {
      // the distcp has been submitted.
      if (job.isComplete()) {
        jobId = null; // unset jobId because the job is done.
        if (job.isSuccessful()) {
          stage = Stage.FINISH;
          return;
        } else {
          throw new IOException(
              "Final DistCp failed. Failure: " + job.getFailureInfo());
        }
      } else {
        throw new RetryException();
      }
    } else {
      submitDiffDistCp();
    }
  }

  void finish() throws IOException {
    if (!useMountReadOnly) {
      restorePermission();
    }
    if (srcFs.exists(src)) {
      cleanupSnapshot(srcFs, src);
    }
    if (dstFs.exists(dst)) {
      cleanupSnapshot(dstFs, dst);
    }
  }

  @VisibleForTesting
  Stage getStage() {
    return stage;
  }

  @VisibleForTesting
  void setStage(Stage stage) {
    this.stage = stage;
  }

  private void submitDiffDistCp() throws IOException {
    enableSnapshot(dstFs, dst);
    deleteSnapshot(srcFs, src, LAST_SNAPSHOT_NAME);
    deleteSnapshot(dstFs, dst, LAST_SNAPSHOT_NAME);
    dstFs.createSnapshot(dst, LAST_SNAPSHOT_NAME);
    srcFs.renameSnapshot(src, CURRENT_SNAPSHOT_NAME, LAST_SNAPSHOT_NAME);
    srcFs.createSnapshot(src, CURRENT_SNAPSHOT_NAME);
    jobId = submitDistCpJob(src.toString(), dst.toString(), true);
  }

  /**
   * Close all open files. Block until all the files are closed.
   */
  private void closeAllOpenFiles(DistributedFileSystem dfs, Path path)
      throws IOException {
    String pathStr = path.toUri().getPath();
    while (true) {
      RemoteIterator<OpenFileEntry> iterator =
          dfs.listOpenFiles(EnumSet.of(OpenFilesType.ALL_OPEN_FILES), pathStr);
      if (!iterator.hasNext()) { // all files has been closed.
        break;
      }
      while (iterator.hasNext()) {
        OpenFileEntry e = iterator.next();
        try {
          srcFs.recoverLease(new Path(e.getFilePath()));
        } catch (IOException re) {
          // ignore recoverLease error.
        }
      }
    }
  }

  /**
   * Verify whether the src has changed since CURRENT_SNAPSHOT_NAME snapshot.
   *
   * @return true if the src has changed.
   */
  private boolean verifyDiff() throws IOException {
    SnapshotDiffReport diffReport =
        srcFs.getSnapshotDiffReport(src, CURRENT_SNAPSHOT_NAME, "");
    return diffReport.getDiffList().size() > 0;
  }

  /**
   * Verify whether there is any open files under src.
   *
   * @return true if there are open files.
   */
  private boolean verifyOpenFiles() throws IOException {
    RemoteIterator<OpenFileEntry> iterator = srcFs
        .listOpenFiles(EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
            src.toString());
    return iterator.hasNext();
  }

  private RunningJob getCurrentJob() throws IOException {
    if (jobId != null) {
      return client.getJob(JobID.forName(jobId));
    }
    return null;
  }

  private void cleanUpBeforeInitDistcp() throws IOException {
    if (dstFs.exists(dst)) { // clean up.
      dstFs.delete(dst, true);
    }
    srcFs.allowSnapshot(src);
    if (srcFs.exists(new Path(src, ".snapshot/" + CURRENT_SNAPSHOT_NAME))) {
      srcFs.deleteSnapshot(src, CURRENT_SNAPSHOT_NAME);
    }
  }

  /**
   * Submit distcp job and return jobId.
   */
  private String submitDistCpJob(String src, String dst,
      boolean useSnapshotDiff) throws IOException {
    List<String> command = new ArrayList<>();
    command.addAll(Arrays
        .asList(new String[] {"-async", "-update", "-append", "-pruxgpcab"}));
    if (useSnapshotDiff) {
      command.add("-diff");
      command.add(LAST_SNAPSHOT_NAME);
      command.add(CURRENT_SNAPSHOT_NAME);
    }
    command.add("-m");
    command.add(mapNum + "");
    command.add("-bandwidth");
    command.add(bandWidth + "");
    command.add(src);
    command.add(dst);

    Configuration config = new Configuration(conf);
    DistCp distCp;
    try {
      distCp = new DistCp(config,
          OptionsParser.parse(command.toArray(new String[]{})));
      Job job = distCp.createAndSubmitJob();
      LOG.info("Submit distcp job={}", job);
      return job.getJobID().toString();
    } catch (Exception e) {
      throw new IOException("Submit job failed.", e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    context.write(out);
    if (jobId == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Text.writeString(out, jobId);
    }
    out.writeInt(stage.ordinal());
    if (fPerm == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeShort(fPerm.toShort());
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
    super.readFields(in);
    context = new FedBalanceContext();
    context.readFields(in);
    src = context.getSrc();
    dst = context.getDst();
    conf = context.getConf();
    if (in.readBoolean()) {
      jobId = Text.readString(in);
    }
    stage = Stage.values()[in.readInt()];
    if (in.readBoolean()) {
      fPerm = FsPermission.read(in);
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
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
    dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
    mapNum =
        conf.getInt(DISTCP_PROCEDURE_MAP_NUM, DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
    bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
        DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
    forceCloseOpenFiles = context.getForceCloseOpenFiles();
    useMountReadOnly = context.getUseMountReadOnly();
    this.client = new JobClient(conf);
  }

  private static void enableSnapshot(DistributedFileSystem dfs, Path path)
      throws IOException {
    if (!dfs.exists(new Path(path, ".snapshot"))) {
      dfs.allowSnapshot(path);
    }
  }

  private static void deleteSnapshot(DistributedFileSystem dfs, Path path,
      String snapshotName) throws IOException {
    Path snapshot = new Path(path, ".snapshot/" + snapshotName);
    if (dfs.exists(snapshot)) {
      dfs.deleteSnapshot(path, snapshotName);
    }
  }

  private static void cleanupSnapshot(DistributedFileSystem dfs, Path path)
      throws IOException {
    if (dfs.exists(new Path(path, ".snapshot"))) {
      FileStatus[] status = dfs.listStatus(new Path(path, ".snapshot"));
      for (FileStatus s : status) {
        deleteSnapshot(dfs, path, s.getPath().getName());
      }
      dfs.disallowSnapshot(path);
    }
  }
}