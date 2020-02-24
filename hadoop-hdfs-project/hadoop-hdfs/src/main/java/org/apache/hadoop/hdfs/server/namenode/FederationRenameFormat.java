package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.hfr.IDConsumer;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FederationRenameFormat {

  /**
   * Write out the directory tree with root n in depth-first manner.
   *
   * @param out the stream to write out.
   * @param n the root inode. It could be either INodeFile or INodeDirectory.
   * @return the statistical info.
   */
  public static Counter saveTree(OutputStream out, INode n) throws IOException {
    if (!(n instanceof INodeFile) && !(n instanceof INodeDirectory)) {
      throw new IOException("Only file and directory can be saved out."
          + " Path=" + n.getFullPathName() + " is neither of them.");
    }
    DataOutputStream dataOut = new DataOutputStream(out);
    Counter counter = new Counter();
    save(n, dataOut, counter);
    dataOut.flush();
    return counter;
  }

  private static void save(INode n, DataOutputStream out, Counter counter)
      throws IOException {
    counter.incrConsume(1, 0);
    FSImageFormatPBINode.Saver.save(out, n);
    if (n.isDirectory()) {
      INodeDirectory dir = n.asDirectory();
      ReadOnlyList<INode> children =
          dir.getChildrenList(Snapshot.CURRENT_STATE_ID);
      out.writeInt(children.size());
      for (int i = 0; i < children.size(); i++) {
        save(children.get(i), out, counter);
      }
    } else {
      counter.incrConsume(0,
          n.asFile().computeFileSize() * n.asFile().getFileReplication());
      counter.incrBlk(n.asFile().numBlocks());
    }
  }

  public static INode loadTree(FSNamesystem fsn, InputStream in, IDConsumer ic)
      throws IOException {

  }

  static class Counter {
    private long nsConsume;
    private long dsConsume;
    private int totalBlkNum;

    public Counter() {
      this.nsConsume = 1;
      this.dsConsume = 0;
      this.totalBlkNum = 0;
    }

    public long getNsConsume() {
      return nsConsume;
    }

    public long getDsConsume() {
      return dsConsume;
    }

    public int getTotalBlkNum() {
      return totalBlkNum;
    }

    public void incrConsume(long nsConsume, long dsConsume) {
      this.nsConsume += nsConsume;
      this.dsConsume += dsConsume;
    }

    public void incrBlk(int blkNum) {
      totalBlkNum += blkNum;
    }
  }
}
