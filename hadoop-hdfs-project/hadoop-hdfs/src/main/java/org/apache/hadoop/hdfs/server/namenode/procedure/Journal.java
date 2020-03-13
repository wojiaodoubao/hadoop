package org.apache.hadoop.hdfs.server.namenode.procedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * TODO:修改这个注释
 */
public interface Journal {
  //TODO:add annotation.
  public void saveJob(Job job) throws IOException;
  public void recoverJob(Job job) throws IOException;
  public Job[] listAllJobs() throws IOException;
  public void clear(Job job) throws IOException;
}
