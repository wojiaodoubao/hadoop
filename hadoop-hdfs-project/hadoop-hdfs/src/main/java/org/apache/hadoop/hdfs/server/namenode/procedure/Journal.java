package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.conf.Configurable;

import java.io.IOException;

/**
 * The Journal of the state machine. It handles the job persistence and recover.
 */
public interface Journal extends Configurable {

  /**
   * Save journal of this job.
   */
  void saveJob(Job job) throws IOException;

  /**
   * Recover the job from journal.
   */
  void recoverJob(Job job) throws IOException;

  /**
   * List all unfinished jobs.
   */
  Job[] listAllJobs() throws IOException;

  /**
   * Clear all the journals of this job.
   */
  void clear(Job job) throws IOException;
}
