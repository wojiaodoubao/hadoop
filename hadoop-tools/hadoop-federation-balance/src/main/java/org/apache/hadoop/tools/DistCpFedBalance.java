package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.server.namenode.procedure.Procedure;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.tools.DistCpFedBalance.DistCpProcedure.Stage.DIFF_DISTCP;
import static org.apache.hadoop.tools.DistCpFedBalance.DistCpProcedure.Stage.INIT_DISTCP;
import static org.apache.hadoop.tools.FedBalanceConfigs.*;

/**
 * Super user needed.
 */
public class DistCpFedBalance {

  static class FedBalanceContext implements Writable {

    private Path src;
    private Path dst;
    private Configuration conf;

    public Configuration getConf() {
      return conf;
    }

    public Path getSrc() {
      return src;
    }

    public Path getDst() {
      return dst;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
  }

  static class DistCpProcedure extends Procedure {

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
    private JobClient client;
    private DistributedFileSystem srcFs;
    private DistributedFileSystem dstFs;

    public DistCpProcedure() {}

    public DistCpProcedure(FedBalanceContext context) throws IOException {
      this.context = context;
      this.conf = context.getConf();
      this.client = new JobClient(conf);
      this.stage = Stage.PRE_CHECK;
      this.mapNum = conf.getInt(DISTCP_PROCEDURE_MAP_NUM,
          DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
      this.bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
          DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
      this.forceCloseOpenFiles =
          conf.getBoolean(DISTCP_PROCEDURE_FORCE_CLOSE_OPEN_FILES, false);
      this.srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
      this.dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
    }

    @Override
    public boolean execute(Procedure lastProcedure)
        throws RetryException, IOException {
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
        return false;
      case FINISH:
        return true;
      }
      return false;
    }

    private void preCheck() throws IOException {
      DistributedFileSystem srcFs =
          (DistributedFileSystem) context.getSrc().getFileSystem(conf);
      DistributedFileSystem dstFs =
          (DistributedFileSystem) context.getDst().getFileSystem(conf);
      FileStatus status = srcFs.getFileStatus(context.getSrc());
      if (!status.isDirectory()) {
        throw new IOException(context.getSrc() + " doesn't exist.");
      }
      if (dstFs.exists(context.getDst())) {
        throw new IOException(context.getDst() + " already exists.");
      }
      stage = INIT_DISTCP;
    }

    private void initDistCp() throws IOException, RetryException {
      RunningJob job = getCurrentJob();
      if (job != null) {
        // the distcp has been submitted.
        if (job.isComplete()) {
          jobId = null;// unset jobId because the job is done.
          if (job.isSuccessful()) {
            stage = DIFF_DISTCP;
            return;
          } else {
            LOG.warn("DistCp failed. Failure: " + job.getFailureInfo());
          }
        } else {
          throw new RetryException();
        }
      }
      cleanUpBeforeInitDistcp();
      srcFs.createSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME);
      jobId = submitDistCpJob(
          context.getSrc().toString() + "./snapshot/" + CURRENT_SNAPSHOT_NAME,
          context.getDst().toString(), false);
    }

    private void diffDistCp() throws IOException, RetryException {
      RunningJob job = getCurrentJob();
      if (job != null) {
        if (job.isComplete()) {
          jobId = null;
          if (job.isSuccessful()) {
            LOG.info("DistCp succeeded. jobId={}", job.getFailureInfo());
          } else {
            LOG.warn("DistCp failed. jobId={} failure={}", job.getID(),
                job.getFailureInfo());
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
        if (!dstFs.exists(new Path(context.getDst(), ".snapshot"))) {
          dstFs.allowSnapshot(context.getDst());
        }
        if (srcFs.exists(
            new Path(context.getSrc(), ".snapshot/" + LAST_SNAPSHOT_NAME))) {
          srcFs.deleteSnapshot(context.getSrc(), LAST_SNAPSHOT_NAME);
        }
        if (dstFs.exists(
            new Path(context.getDst(), ".snapshot/" + LAST_SNAPSHOT_NAME))) {
          dstFs.deleteSnapshot(context.getDst(), LAST_SNAPSHOT_NAME);
        }
        dstFs.createSnapshot(context.getDst(), LAST_SNAPSHOT_NAME);
        srcFs.renameSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME,
            LAST_SNAPSHOT_NAME);
        srcFs.createSnapshot(context.getSrc(), CURRENT_SNAPSHOT_NAME);
        jobId = submitDistCpJob(context.getSrc().toString(),
            context.getDst().toString(), true);
      }
    }

    /**
     * Verify whether the src has changed since CURRENT_SNAPSHOT_NAME snapshot.
     *
     * @return true if the src has changed.
     */
    private boolean verifyDiff() throws IOException {
      SnapshotDiffReport diffReport = srcFs
          .getSnapshotDiffReport(context.getSrc(), CURRENT_SNAPSHOT_NAME,
              "");
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
      srcFs.deleteSnapshot(context.getSrc(), LAST_SNAPSHOT_NAME);
    }

    /**
     * Submit distcp job and return jobId;
     */
    private String submitDistCpJob(String src, String dst,
        boolean useSnapshotDiff) throws IOException {
      List<String> command = Arrays.asList(
          new String[] { "-async", "-update", "-append", "-pruxgpcab" });
      if (useSnapshotDiff) {
        command.add("-diff");
        command.add(LAST_SNAPSHOT_NAME);
        command.add(CURRENT_SNAPSHOT_NAME);
      }
      command.add("-m");
      command.add(mapNum+"");
      command.add("-bandwidth");
      command.add(bandWidth+"");
      command.add(src);
      command.add(dst);

      int exitCode;
      Configuration config = new Configuration(conf);
      try {
        exitCode = ToolRunner
            .run(config, new DistCp(), command.toArray(new String[] {}));
      } catch (Exception e) {
        throw new IOException("Submit job failed.", e);
      }
      if (exitCode != 0) {
        throw new IOException("Exit code is not zero. exit code=" + exitCode);
      }
      String jobID = config.get(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID);
      return jobID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      context.write(out);
      Text.writeString(out, jobId);
      out.write(stage.ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      context.readFields(in);
      jobId = Text.readString(in);
      stage = Stage.values()[in.readInt()];
      srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
      dstFs = (DistributedFileSystem) context.getDst().getFileSystem(conf);
      mapNum = conf.getInt(DISTCP_PROCEDURE_MAP_NUM,
          DISTCP_PROCEDURE_MAP_NUM_DEFAULT);
      bandWidth = conf.getInt(DISTCP_PROCEDURE_BAND_WIDTH_LIMIT,
          DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT);
      forceCloseOpenFiles =
          conf.getBoolean(DISTCP_PROCEDURE_FORCE_CLOSE_OPEN_FILES, false);
    }
  }

  static class PrepareProcedure extends Procedure {

    public PrepareProcedure() {}

    @Override
    public boolean execute(Procedure lastProcedure)
        throws RetryException, IOException {
      return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
  }
}
