package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

public class LockHelper {
  private FSNamesystem namesystem;
  private FSDirectory dir;
  private int nsTimes;
  private int dirTimes;
  private boolean removeDone;

  public LockHelper(FSNamesystem namesystem, FSDirectory dir) {
    this.namesystem = namesystem;
    this.dir = dir;
    this.removeDone = false;
    this.nsTimes = 0;
    this.dirTimes = 0;
  }

  public boolean removeLock() {
    if (removeDone) {
      return false;
    }
    while (namesystem.hasWriteLock()) {
      nsTimes++;
      namesystem.writeUnlock();
    }
    while (dir.hasWriteLock()) {
      dirTimes++;
      dir.writeUnlock();
    }
    removeDone = true;
    return true;
  }

  public boolean restoreLock() {
    if (!removeDone) {
      return false;
    }
    while (nsTimes > 0) {
      nsTimes--;
      namesystem.writeLock();
    }
    while (dirTimes > 0) {
      dirTimes--;
      dir.writeLock();
    }
    removeDone = false;
    return true;
  }
}
