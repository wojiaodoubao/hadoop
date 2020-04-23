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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.procedure.BalanceProcedure;
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

import static org.apache.hadoop.tools.FedBalanceConfigs.*;

/**
 * Copy data through distcp. Super user privilege needed.
 *
 * PRE_CHECK               :pre-check of src and dst.
 * INIT_DISTCP             :the first round of distcp.
 * DIFF_DISTCP             :copy snapshot diff round by round until there is
 *                          no diff.
 * FINAL_DISTCP(optional)  :close all open files and do the final round distcp.
 * FINISH                  :procedure finish.
 */
public class DistCpProcedure extends BalanceProcedure {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpProcedure.class);

  enum Stage {
    PRE_CHECK, INIT_DISTCP, DIFF_DISTCP, FINAL_DISTCP, FINISH
  }

  private FedBalanceContext context;
  private Configuration conf;
  private int mapNum;
  private int bandWidth;
  private String jobId;
  private Stage stage;
  private boolean forceCloseOpenFiles;
  private FsPermission fPerm;
  private AclStatus acl;
  private boolean savePermission;
  private JobClient client;
  private DistributedFileSystem srcFs;
  private DistributedFileSystem dstFs;

  public DistCpProcedure() {
  }

  public DistCpProcedure(String name, String nextProcedure, long delayDuration,
      FedBalanceContext context) throws IOException {
    super(name, nextProcedure, delayDuration);
    this.context = context;
    this.conf = context.getConf();
    this.client = new JobClient(conf);
    this.stage = Stage.PRE_CHECK;
    this.mapNum =
        conf.getInt(DISTCP_PROCEDURE_MAP_NUM, DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
    this.bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
        DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
    this.forceCloseOpenFiles =
        conf.getBoolean(DISTCP_PROCEDURE_FORCE_CLOSE_OPEN_FILES, false);
    this.savePermission = true;
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
    dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
  }

  @Override
  public boolean execute(BalanceProcedure lastProcedure)
      throws RetryException, IOException {
    LOG.info("Stage=" + stage.name());
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
    case FINAL_DISTCP:
      finalDistCp();
      return false;
    case FINISH:
      finish();
      return true;
    }
    return false;
  }

  void preCheck() throws IOException {
    FileStatus status = srcFs.getFileStatus(context.getSrc());
    if (!status.isDirectory()) {
      throw new IOException(context.getSrc() + " doesn't exist.");
    }
    if (dstFs.exists(context.getDst())) {
      throw new IOException(context.getDst() + " already exists.");
    }
    if (srcFs.exists(new Path(context.getSrc(), ".snapshot"))) {
      throw new IOException(context.getSrc() + " shouldn't enable snapshot.");
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
        jobId = null;// unset jobId because the job is done.
        if (job.isSuccessful()) {
          stage = Stage.DIFF_DISTCP;
          return;
        } else {
          LOG.warn("DistCp failed. Failure: " + job.getFailureInfo());
        }
      } else {
        throw new RetryException();
      }
    } else {
      cleanUpBeforeInitDistcp();
      srcFs.createSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME);
      jobId = submitDistCpJob(
          context.getSrc().toString() + "/.snapshot/" + CURRENT_SNAPSHOT_NAME,
          context.getDst().toString(), false);
    }
  }

  /**
   * The distcp copying diffs between LAST_SNAPSHOT_NAME and CURRENT_SNAPSHOT_NAME.
   */
  void diffDistCp() throws IOException, RetryException {
    RunningJob job = getCurrentJob();
    if (job != null) {
      if (job.isComplete()) {
        jobId = null;
        if (job.isSuccessful()) {
          LOG.info("DistCp succeeded. jobId={}", job.getFailureInfo());
        } else {
          throw new IOException(
              "DistCp failed. jobId=" + job.getID() + "failure=" + job
                  .getFailureInfo());
        }
      } else {
        throw new RetryException();// wait job complete.
      }
    } else if (!verifyDiff()) {
      if (!verifyOpenFiles()) {
        stage = Stage.FINISH;
      } else if (forceCloseOpenFiles) {
        stage = Stage.FINAL_DISTCP;
      } else {
        throw new RetryException();
      }
    } else {
      submitDiffDistCp();
    }
  }

  /**
   * Close all open files then submit the distcp with -diff.
   */
  void finalDistCp() throws IOException, RetryException {
    // Cancel x permission, close all open files then do the final distcp.
    if (savePermission) {
      FileStatus status = srcFs.getFileStatus(context.getSrc());
      fPerm = status.getPermission();
      acl = srcFs.getAclStatus(context.getSrc());
      srcFs.setPermission(context.getSrc(),
          FsPermission.createImmutable((short) 0));
      savePermission = false;
    }
    closeAllOpenFiles(srcFs, context.getSrc());
    // final distcp.
    RunningJob job = getCurrentJob();
    if (job != null) {
      // the distcp has been submitted.
      if (job.isComplete()) {
        jobId = null;// unset jobId because the job is done.
        if (job.isSuccessful()) {
          // restore permission.
          dstFs.removeAcl(context.getDst());
          if (acl != null) {
            dstFs.modifyAclEntries(context.getDst(), acl.getEntries());
          }
          dstFs.setPermission(context.getDst(), fPerm);
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
    if (srcFs.exists(context.getSrc())) {
      cleanupSnapshot(srcFs, context.getSrc());
    }
    if (dstFs.exists(context.getDst())) {
      cleanupSnapshot(dstFs, context.getDst());
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
    enableSnapshot(dstFs, context.getDst());
    deleteSnapshot(srcFs, context.getSrc(), LAST_SNAPSHOT_NAME);
    deleteSnapshot(dstFs, context.getDst(), LAST_SNAPSHOT_NAME);
    dstFs.createSnapshot(context.getDst(), LAST_SNAPSHOT_NAME);
    srcFs.renameSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME,
        LAST_SNAPSHOT_NAME);
    srcFs.createSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME);
    jobId = submitDistCpJob(context.getSrc().toString(),
        context.getDst().toString(), true);
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
    SnapshotDiffReport diffReport = srcFs
        .getSnapshotDiffReport(context.getSrc(), CURRENT_SNAPSHOT_NAME, "");
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
            context.getSrc().toString());
    return iterator.hasNext();
  }

  private RunningJob getCurrentJob() throws IOException {
    if (jobId != null) {
      return client.getJob(JobID.forName(jobId));
    }
    return null;
  }

  private void cleanUpBeforeInitDistcp() throws IOException {
    if (dstFs.exists(context.getDst())) { // clean up.
      dstFs.delete(context.getDst(), true);
    }
    srcFs.allowSnapshot(context.getSrc());
    if (srcFs.exists(
        new Path(context.getSrc(), ".snapshot/" + CURRENT_SNAPSHOT_NAME))) {
      srcFs.deleteSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME);
    }
  }

  /**
   * Submit distcp job and return jobId;
   */
  private String submitDistCpJob(String src, String dst,
      boolean useSnapshotDiff) throws IOException {
    List<String> command = new ArrayList<>();
    command.addAll(Arrays
        .asList(new String[] { "-async", "-update", "-append", "-pruxgpcab" }));
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
      LOG.info("Submit distcp job:" + job);
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
    out.writeBoolean(savePermission);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    context = new FedBalanceContext();
    context.readFields(in);
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
    savePermission = in.readBoolean();
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
    dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
    mapNum =
        conf.getInt(DISTCP_PROCEDURE_MAP_NUM, DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
    bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
        DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
    forceCloseOpenFiles =
        conf.getBoolean(DISTCP_PROCEDURE_FORCE_CLOSE_OPEN_FILES, false);
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