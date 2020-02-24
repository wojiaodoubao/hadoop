package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;

public class TreeMeta {
  long nsConsume;       // the total inode of this federation rename.
  long dsConsume;       // the total space of this federation rename.
  int totalBlock;       // the total block num of this federation rename.
  public static class Builder {
    long nsConsume;
    long dsConsume;
    int totalBlock;
    public Builder setNsConsume(long nsConsume) {
      this.nsConsume = nsConsume;
      return this;
    }
    public Builder setDsConsume(long dsConsume) {
      this.dsConsume = dsConsume;
      return this;
    }
    public Builder setTotalBlock(int totalBlock) {
      this.totalBlock = totalBlock;
      return this;
    }
    public TreeMeta build() {
      return new TreeMeta(nsConsume, dsConsume, totalBlock);
    }
  }
  private TreeMeta(long nsConsume, long dsConsume, int totalBlock) {
    this.nsConsume = nsConsume;
    this.dsConsume = dsConsume;
    this.totalBlock = totalBlock;
  }
  public long getNsConsume() {
    return nsConsume;
  }
  public long getDsConsume() {
    return dsConsume;
  }
  public int getTotalBlock() {
    return totalBlock;
  }
  public QuotaCounts getQuotaConsume() {
    return new QuotaCounts.Builder().nameSpace(nsConsume)
        .storageSpace(dsConsume).build();
  }
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TreeMeta) {
      TreeMeta tm = (TreeMeta) obj;
      return nsConsume == tm.nsConsume && dsConsume == tm.dsConsume
          && totalBlock == tm.totalBlock;
    }
    return false;
  }
}
