package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.SequentialNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.SCHEDULER_BASE_URI;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.TMP_TAIL;
import static org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureConfigKeys.JOB_PREFIX;

/**
 * The procedure journal based on HDFS.
 */
public class HDFSJournal implements Journal {

  public static final Logger LOG = LoggerFactory.getLogger(HDFSJournal.class);

  public static class IdGenerator extends SequentialNumber {
    protected IdGenerator(long initialValue) {
      super(initialValue);
    }
  }

  private URI workUri;
  private Configuration conf;
  private IdGenerator generator;

  public HDFSJournal(Configuration conf) throws URISyntaxException {
    this.workUri = new URI(conf.get(SCHEDULER_BASE_URI));
    this.conf = conf;
    this.generator = new IdGenerator(System.currentTimeMillis());
  }

  /**
   * Save job journal to HDFS.
   */
  public void saveJob(Job job) throws IOException {
    Path jobFile = getNextJob(job);
    Path tmpJobFile =
        new Path(jobFile.getParent(), jobFile.getName() + TMP_TAIL);
    FSDataOutputStream out = null;
    try {
      FileSystem fs = FileSystem.get(workUri, conf);
      out = fs.create(tmpJobFile);
      job.write(new DataOutputStream(out));
      out.close();
      out = null;
      fs.rename(tmpJobFile, jobFile);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
        }
      }
    }
  }

  /**
   * Recover job from journal on HDFS.
   */
  public void recoverJob(Job job) throws IOException {
    assert job.getId() != null;
    FSDataInputStream in = null;
    try {
      Path logPath = getLatestJob(job);
      FileSystem fs = FileSystem.get(workUri, conf);
      in = fs.open(logPath);
      job.readFields(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  @Override
  public Job[] listAllJobs() throws IOException {
    FileSystem fs = FileSystem.get(workUri, conf);
    FileStatus status = null;
    try {
      status = fs.getFileStatus(new Path(workUri.getPath()));
    } catch (FileNotFoundException e) {
    }
    if (status == null) {
      fs.mkdirs(new Path(workUri.getPath()));
    } else if (!status.isDirectory()) {
      throw new RuntimeException(workUri + " must be a directory.");
    }
    FileStatus[] statuses = fs.listStatus(new Path(workUri.getPath()));
    Job[] jobs = new Job[statuses.length];
    for (int i = 0; i < statuses.length; i++) {
      if (statuses[i].isDirectory()) {
        jobs[i] = new Job.Builder<>().build();
        jobs[i].setId(statuses[i].getPath().getName());
      }
    }
    return jobs;
  }

  @Override
  public void clear(Job job) throws IOException {
    Path jobBase = getJobBaseDir(job);
    FileSystem fs = FileSystem.get(workUri, conf);
    if (fs.exists(jobBase)) {
      fs.delete(jobBase, true);
    }
  }

  private Path getJobBaseDir(Job job) {
    String jobId = job.getId();
    return new Path(workUri.getPath(), jobId);
  }

  private Path getNextJob(Job job) {
    Path basePath = getJobBaseDir(job);
    Path logPath = new Path(basePath, JOB_PREFIX + generator.nextValue());
    return logPath;
  }

  private Path getLatestJob(Job job) throws IOException {
    Path latestFile = null;
    Path basePath = getJobBaseDir(job);
    FileSystem fs = FileSystem.get(workUri, conf);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(basePath, false);
    while (iterator.hasNext()) {
      FileStatus status = iterator.next();
      String fileName = status.getPath().getName();
      if (fileName.startsWith(JOB_PREFIX) && !fileName.contains(TMP_TAIL)) {
        if (latestFile == null) {
          latestFile = status.getPath();
        } else if (latestFile.getName().compareTo(fileName) <= 0) {
          latestFile = status.getPath();
        }
      }
    }
    return latestFile;
  }
}
