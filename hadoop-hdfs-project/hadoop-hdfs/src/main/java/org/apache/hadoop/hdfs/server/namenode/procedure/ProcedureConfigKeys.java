package org.apache.hadoop.hdfs.server.namenode.procedure;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class contains constants for configuration keys and default values
 * used in hdfs procedure.
 */
@InterfaceAudience.Private
public class ProcedureConfigKeys {
  public static final String WORK_THREAD_NUM = "hadoop.hdfs.procedure.work.thread.num";
  public static final int WORK_THREAD_NUM_DEFAULT = 10;
  public static final String SCHEDULER_BASE_URI =
      "hadoop.hdfs.job.scheduler.base.uri";
  public static final String JOB_PREFIX = "JOB-";
  public static final String TMP_TAIL = ".tmp";
}
