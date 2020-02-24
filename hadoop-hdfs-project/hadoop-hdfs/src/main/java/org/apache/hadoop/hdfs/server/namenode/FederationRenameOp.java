package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.hfr.BasicInfo;
import org.apache.hadoop.hdfs.server.namenode.hfr.IDConsumer;
import org.apache.hadoop.hdfs.server.namenode.hfr.LockHelper;
import org.apache.hadoop.hdfs.server.namenode.hfr.TreeMeta;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Move path across federation namespaces.
 */
public class FederationRenameOp {

  /**
   * Save tree out to the remote storage. The whole process is lock free.
   */
  static String unprotectedSaveSubTree(FSNamesystem fsn, String path)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    // write out the TREE file.
    INodesInPath srcIIP = fsd.resolvePath(null, path, DirOp.READ);
    INode srcInode = srcIIP.getLastINode();
    if (srcInode == null) {
      throw new IOException(path + " doesn't exist.");
    }
    TreeMeta tMeta = frpa.writeSubTree(srcInode, bi);
    // write TREE-META.
    frpa.saveTreeMeta(bi.getContextURI(), tMeta);
  }

  // graft tree to safeDst.
  static boolean graftSubTree(FSNamesystem fsn, BasicInfo bi, TreeMeta tm,
      boolean logRetryCache) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsn.hasWriteLock();
    String dst = fsd.normalizePath(bi.getSafeDstPath());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.graftSubTree: " + bi);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    String actualDst = resolveActualDst(fsd, bi.getSrcPath(), dst);
    graftSubTree(fsn, bi, tm, actualDst, false, logRetryCache, null);
    fsd.getEditLog()
        .logGraftSubTree(bi.getContextURI().toString(), logRetryCache);
    return true;
  }

  static boolean graftSubTreeEditLog(FSNamesystem fsn, BasicInfo bi,
      TreeMeta tm, IDConsumer editConsumer) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert  fsn.hasWriteLock();
    String dst = fsd.normalizePath(bi.getSafeDstPath());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.graftSubTree: " + bi);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    String actualDst = resolveActualDst(fsd, bi.getSrcPath(), dst);
    graftSubTree(fsn, bi, tm, actualDst, true, false, editConsumer);
    return true;
  }

  private static void graftSubTree(FSNamesystem fsn, BasicInfo info,
      TreeMeta tm, String actualDst, boolean editlog, boolean logRetryCache,
      IDConsumer editConsumer) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    fsd.writeLock();
    LockHelper lockHelper = new LockHelper(fsd.getFSNamesystem(), fsd);
    INodesInPath dstIIP = fsd.resolvePath(null, actualDst, DirOp.WRITE_LINK);
    checkRenameDstValid(dstIIP, actualDst);
    final String parentPath = dstIIP.getParentPath();
    fsd.verifyMaxComponentLength(dstIIP.getLastLocalName(),
        dstIIP.getParentPath());
    fsd.verifyMaxDirItems(dstIIP.getINode(-2).asDirectory(), parentPath);
    // 1. update count
    fsd.updateCount(dstIIP, dstIIP.length() - 1, tm.getQuotaConsume(), true);
    // 2. pre-allocate inodeId & blkId & GS
    IDConsumer ic;
    if (editlog) {
      ic = editConsumer;
    } else {
      ic = preAllocateIDs(fsn, tm.getNsConsume(), tm.getTotalBlock(),
          logRetryCache);
    }
    INode gNode;
    try {
      // give up lock
      lockHelper.removeLock();
      // logSync of the pre-allocate edit log.
      if (!editlog) {
        fsd.getFSNamesystem().getEditLog().logSync();
      }
      // 3.graft without lock(no quota check)
      gNode = frpa.readSubTree(fsn, info, ic);
      // 4.write BLOCK,INODE,GS map file.
      if (!editlog) {
        frpa.saveINodeIdMap(info.getContextURI(), ic.getInodeMap());
        frpa.saveBlockMap(info.getContextURI(), ic.getBlkMap());
      }
      lockHelper.restoreLock();
      dstIIP = fsd.resolvePath(null, actualDst, DirOp.WRITE_LINK);
      // After re-fetching the lock, we need to check the actualDst again.
      checkRenameDstValid(dstIIP, actualDst);
      // 4. graft all INodes and Blocks to FSDir. Steps below never fail.
      INodeDirectory parent = dstIIP.getINode(-2).asDirectory();
      parent.addChild(gNode);                // graft rNode to parent.
      unprotectedGraftInodeBlockToDir(fsn, fsd, // add blk & inode to inner-map.
          gNode);
    } catch (IOException e) {
      lockHelper.restoreLock();
      // recover the consume
      fsd.updateCount(dstIIP, dstIIP.length() - 1,
          tm.getQuotaConsume().negation(), true);
      // No need to clear rNode. Let gc handle it. No need to clear
      // inodes & blks from the tree and inner-map as it never fails.
      throw e;
    } finally {
      lockHelper.restoreLock();
      fsd.writeUnlock();
    }
  }

  /**
   * If dst exists and is dir, the actual path is dst/src.getName().
   * If dst not exists and dst's parent is dir, the actual path is dst.
   * otherwise throwing IOException.
   */
  static String resolveActualDst(FSDirectory fsd, String src, String dst)
      throws IOException {
    INodesInPath iip = fsd.resolvePath(null, dst, DirOp.READ);
    INode lastInode = iip.getLastINode();
    if (lastInode != null) {
      if (lastInode.isDirectory()) {
        return dst + Path.SEPARATOR + new Path(src).getName();
      } else {
        throw new IOException("Dst path already exists.");
      }
    } else {
      INode parentInode = iip.getINode(-2);
      if (parentInode != null && parentInode.isDirectory()) {
        return dst;
      } else {
        throw new IOException("Dst parent is either not exist or not a dir.");
      }
    }
  }

  static void checkRenameDstValid(INodesInPath dstIIP, String dst) throws IOException {
    if (dstIIP.getLastINode() != null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to graft " + dst
              + " because destination exists");
      throw new IOException(
          "failed to graft " + dst + " because destination exists");
    }
    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null || !dstParent.isDirectory()) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + "DIR* FSDirectory.unprotectedRenameTo: " + "failed to graft " + dst
          + " because des's parent does not exist or is not dir.");
      throw new IOException("failed to graft " + dst
          + " because destination's parent does not exist or is not dir.");
    }
  }

  // this method never fail.
  static void unprotectedGraftInodeBlockToDir(FSNamesystem fsn, FSDirectory fsd,
      INode root) {
    LinkedList<INode> queue = new LinkedList<>();
    queue.addLast(root);
    while (!queue.isEmpty()) {
      INode node = queue.removeFirst();
      fsd.addToInodeMap(node);
      if (node.isFile()) {
        BlockInfo[] blocks = node.asFile().getBlocks();
        for (BlockInfo bi : blocks) {
          // the blk in blocks doesn't exist in BlockManager, so there is no
          // need to handle the return value of addBlockCollection.
          fsn.getBlockManager().addBlockCollection(bi, node.asFile());
        }
      } else {
        ReadOnlyList<INode> children =
            node.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
        for (int i = 0; i < children.size(); i++) {
          queue.addLast(children.get(i));
        }
      }
    }
  }

  /**
   * Pre-allocate inodeId, blkId and GS.
   */
  static IDConsumer preAllocateIDs(FSNamesystem fsn, long inodeSize,
      int blkSize, boolean logRetryCache) throws IOException {
    return preAllocateIDs(fsn, inodeSize, blkSize, logRetryCache, true);
  }

  static IDConsumer preAllocateIDs(FSNamesystem fsn, long inodeSize,
      int blkSize, boolean logRetryCache, boolean editLog) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsn.hasWriteLock();
    assert fsd.hasWriteLock();
    long inodeIdStart = fsd.getLastInodeId() + 1;
    fsn.allocateInodeId(inodeSize);
    long[] blkIds = fsn.allocateBlockIdWithoutLog(blkSize);
    long gsStart = fsn.getCurrentGenStamp(false) + 1;
    fsn.allocateGenerationStamp(false, blkSize);
    if (editLog) {
      fsn.getEditLog().logPreAllocateIDs(inodeSize, blkSize, logRetryCache);
    }
    return new IDConsumer(inodeIdStart, inodeSize, blkIds, gsStart, blkSize);
  }

  static public FederationRenamePeristAdapter getFRPA() {
    return frpa;
  }
}
