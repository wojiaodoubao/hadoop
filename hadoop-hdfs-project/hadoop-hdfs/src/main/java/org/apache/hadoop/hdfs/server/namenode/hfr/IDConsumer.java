package org.apache.hadoop.hdfs.server.namenode.hfr;

import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IDConsumer {
  long nextInode;
  long inodeEnd;
  long[] blkIds;
  int blkIndex;
  long nextGS;
  long gsEnd;
  Map<Long, Long> inodeMap;
  Map<Long, Block> blkMap;
  final boolean USE_MAP;

  // TODO: Reserved for Parallel optimization of saving & loading tree in FederationRenameFormatV2(EditLog).
  public IDConsumer(Map<Long, Long> inodeMap, Map<Long, Block> blkMap) {
    this.inodeMap = inodeMap;
    this.blkMap = blkMap;
    USE_MAP = true;
  }

  public IDConsumer(long inodeIdStart, long inodeIdSize, long[] blkIds,
      long gsStart, long gsSize) {
    this.nextInode = inodeIdStart;
    this.inodeEnd = inodeIdStart + inodeIdSize;
    this.blkIds = blkIds;
    this.blkIndex = 0;
    this.nextGS = gsStart;
    this.gsEnd = gsStart + gsSize;
    this.inodeMap = new HashMap<>();
    this.blkMap = new HashMap<>();
    USE_MAP = false;
  }

  public long inodeId(long srcId) throws IOException {
    if (USE_MAP) {
      Long dstId = inodeMap.get(srcId);
      if (dstId == null) {
        throw new IOException("Invalid srcId " + srcId);
      }
      return dstId;
    } else {
      if (nextInode >= inodeEnd) {
        throw new IOException("Inode id has run out.");
      }
      long res = nextInode;
      inodeMap.put(srcId, res);
      nextInode++;
      return res;
    }
  }

  public Block block(long srcId, long size) throws IOException {
    if (USE_MAP) {
      Block block = blkMap.get(srcId);
      if (block == null) {
        throw new IOException("Invalid srcId " + srcId);
      }
      return block;
    } else {
      if (blkIndex >= blkIds.length) {
        throw new IOException("Blk id has run out.");
      }
      if (nextGS >= gsEnd) {
        throw new IOException("GS has run out.");
      }
      long nId = blkIds[blkIndex];
      long nGs = nextGS;
      Block res = new Block(nId, size, nGs);
      blkMap.put(srcId, res);
      blkIndex++;
      nextGS++;
      return res;
    }
  }

  public Map<Long, Long> getInodeMap() {
    return inodeMap;
  }

  public Map<Long, Block> getBlkMap() {
    return blkMap;
  }
}
