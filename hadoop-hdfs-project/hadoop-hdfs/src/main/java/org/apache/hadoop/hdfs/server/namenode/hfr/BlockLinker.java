package org.apache.hadoop.hdfs.server.namenode.hfr;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FederationClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.FederationClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FRHelper;
import org.apache.hadoop.hdfs.server.namenode.FederationRenamePeristAdapter;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.protocol.Block.BLOCK_FILE_PREFIX;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATIOIN_RENAME_MAX_HARD_LINK_PER_DN;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATIOIN_RENAME_MAX_HARD_LINK_PER_DN_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_HARD_LINK_THRESHOLD;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_HARD_LINK_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_LINK_THREAD_NUM;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_LINK_THREAD_NUM_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_SLOW_LINK_THRESHOLD;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.FEDERATION_RENAME_SLOW_LINK_THRESHOLD_DEFAULT;

/**
 * Link blocks based on BASIC-INFO and BLOCK-MAP.
 * It's used in FederationRenameV2.
 *
 * TODO:
 * 1.Putting the federation rename code here and some day we'll move them all to
 * RBF. Including the whole Job-Task structure and the FederationRename job.
 * 2.Good thing is Job-Task and FederationRename is all about hdfs admin. It is
 * very easy to update even the interface and SeDe.
 *
 * TODO: [High] Multi-Connections when collecting replica info.
 */
public class BlockLinker {
  public static final Logger LOG = LoggerFactory.getLogger(BlockLinker.class);
  private URI contextURI;
  private FederationRenamePeristAdapter frpa;
  private Configuration conf;
  private BasicInfo info;
  Map<Long, Block> blkMap;
  private final int THREAD_NUM;
  private final int SLOW_LINK_THRESHOLD;

  public BlockLinker(URI contextURI, Configuration conf) {
    this.contextURI = contextURI;
    this.frpa = new FRHelper();
    this.conf = conf;
    this.THREAD_NUM = conf.getInt(FEDERATION_RENAME_LINK_THREAD_NUM,
        FEDERATION_RENAME_LINK_THREAD_NUM_DEFAULT);
    this.SLOW_LINK_THRESHOLD =
        conf.getInt(FEDERATION_RENAME_SLOW_LINK_THRESHOLD,
            FEDERATION_RENAME_SLOW_LINK_THRESHOLD_DEFAULT);
  }

  /**
   * Collect replica info and hard link them. This method waits until all
   * hardlink jobs are done or the replication reaches the threshold.
   *
   * @throws InterruptedException if interrupted while waiting.
   * @throws IOException if hardlink fails.
   */
  public void linkReplicas() throws IOException, InterruptedException {
    info = frpa.getBasicInfo(contextURI);
    blkMap = frpa.getBlockMap(contextURI);
    DistributedFileSystem srcFs =
        (DistributedFileSystem) FileSystem.get(info.getSrc(), conf);
    HardLinkInfo hlInfo = new HardLinkInfo(
        conf.getInt(FEDERATION_RENAME_HARD_LINK_THRESHOLD,
            FEDERATION_RENAME_HARD_LINK_THRESHOLD_DEFAULT),
        info.getDstPoolId());
    int maxHLPerDN = conf.getInt(FEDERATIOIN_RENAME_MAX_HARD_LINK_PER_DN,
        FEDERATIOIN_RENAME_MAX_HARD_LINK_PER_DN_DEFAULT);

    long start = Time.now();
    collectReplicas(srcFs, new Path(info.getSafeSrcPath()), hlInfo, THREAD_NUM);
    LOG.info("compute locations costs " + (Time.now() - start) + "ms.");

    start = Time.now();
    if (hlInfo.pendingBlocks.size() == 0) {
      return;
    }
    doLink(hlInfo, THREAD_NUM, maxHLPerDN);
    LOG.info("call hard link costs " + (Time.now() - start) + "ms.");

    hlInfo.checkAllHardlinkDone();
  }

  class ParallelReplicaCollector extends ParallelConsumer<FileStatus, Object> {
    private DistributedFileSystem srcFs;
    private HardLinkInfo hlInfo;

    ParallelReplicaCollector(DistributedFileSystem srcFs, Path path,
        HardLinkInfo hlInfo, int threadNum) throws IOException {
      super(threadNum);
      this.srcFs = srcFs;
      this.hlInfo = hlInfo;

      add(srcFs.getFileStatus(path));
    }

    @Override
    public void consume(FileStatus s, Object attach) throws Exception {
      if (s.isDirectory()) {
        FileStatus[] statuses = srcFs.listStatus(s.getPath());
        add(statuses);
      } else if (s.isFile()) {
        // TODO: may be a new design for BlocksToDup.
        LocatedBlocks lBlocks =
            srcFs.getClient().getLocatedBlocks(absolute(s), 0, s.getLen());
        hlInfo.add(s, lBlocks);
      }
    }
  }

  void collectReplicas(final DistributedFileSystem srcFs, Path path,
      HardLinkInfo hardLinkInfo, int threadNum)
      throws IOException, InterruptedException {
    ParallelReplicaCollector collector =
        new ParallelReplicaCollector(srcFs, path, hardLinkInfo, threadNum);
    collector.execute();
  }

  /**
   * Do hardlink based on hlInfo. This method waits until all hardlink jobs are
   * done or the replication reaches the threshold. Return true if replication
   * reaches the threshold.
   *
   * @param hlInfo the replicas to be hard linked.
   * @param maxHLPerDN hardlink threads per datanode.
   * @throws InterruptedException if interrupted while waiting.
   */
  boolean doLink(HardLinkInfo hlInfo, int maxThread, int maxHLPerDN)
      throws InterruptedException {
    Map<Long, BlockToLink> pendingBlocks = hlInfo.pendingBlocks;
    int dnNum = hlInfo.hlMap.size();
    int taskNum = dnNum * maxHLPerDN;
    int threads = Math.min(taskNum, maxThread);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 1,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(taskNum));
    LOG.info("Start Hardlink thread-num={} dn-num={} blk-size={}", threads, dnNum,
        hlInfo.pendingBlocks.size());
    try {
      CompletionService<Block[]> linkService =
          new ExecutorCompletionService<>(executor);
      ConcurrentHashMap<Future, LinkTask> futureMap = new ConcurrentHashMap<>();
      // submit hard link tasks.
      for (int i = 0; i < maxHLPerDN; i++) {
        for (Map.Entry<DatanodeInfo, LinkedList<BlocksToDup>> entry : hlInfo.hlMap
            .entrySet()) {
          LinkTask linkTask = createLinkTask(entry.getKey(), entry.getValue());
          Future<Block[]> future = linkService.submit(linkTask);
          futureMap.put(future, linkTask);
        }
      }
      // wait until replication reaches the threshold.
      while (!futureMap.isEmpty() && pendingBlocks.size() > 0) {
        Future<Block[]> future = linkService.take();
        Block[] blocks;
        try {
          blocks = future.get();
        } catch (InterruptedException ite) {
          throw ite;
        } catch (Throwable error) {
          LinkTask linkTask = futureMap.get(future);
          LOG.warn("link block to " + linkTask.dnInfo + " failed.", error);
          continue;
        } finally {
          futureMap.remove(future);
        }
        for (Block b : blocks) {
          BlockToLink btl = pendingBlocks.get(b.getBlockId());
          if (btl != null) {
            synchronized (this) {
              btl.linked++;
              if (btl.linked >= btl.expected) {
                pendingBlocks.remove(b.getBlockId());
              }
            }
          }
        }
      }

      for (Future f : futureMap.keySet()) {
        if (!f.isDone() && !f.isCancelled()) {
          f.cancel(true);
        }
      }
    } finally {
      if (executor != null && !executor.isShutdown()) {
        // there is no need to wait for thread pool terminated, because LinkTask
        // always ends itself after interrupted.
        executor.shutdownNow();
      }
    }
    return pendingBlocks.size() == 0;
  }

  LinkTask createLinkTask(DatanodeInfo dn, LinkedList<BlocksToDup> blkToDup) {
    return new LinkTask(dn, blkToDup);
  }

  class HardLinkInfo {

    private Map<DatanodeInfo, LinkedList<BlocksToDup>> hlMap = new HashMap<>();
    @VisibleForTesting
    Map<Long, BlockToLink> pendingBlocks = new HashMap<>();
    private final int SPLIT_THRESHOLD;
    private final String DST_POOL;

    /**
     * Hardlink to one DN will be separated to many rpcs. Each rpc is restricted
     * to at most SPLIT_THRESHOLD replicas.
     * Since DN handles hardlink in a single thread for each rpc, split rpcs can
     * fast the hard link process.
     */
    public HardLinkInfo(final int splitThreshold, final String dstPoolId) {
      SPLIT_THRESHOLD = splitThreshold;
      DST_POOL = dstPoolId;
    }

    @VisibleForTesting
    HardLinkInfo(Map<DatanodeInfo, LinkedList<BlocksToDup>> hlMap,
        Map<Long, BlockToLink> pendingBlocks, final int splitThreshold,
        final String dstPoolId) {
      this(splitThreshold, dstPoolId);
      this.hlMap = hlMap;
      this.pendingBlocks = pendingBlocks;
    }

    public void add(FileStatus s, LocatedBlocks lBlocks) throws IOException {
      if (lBlocks.getLocatedBlocks().size() == 0) {
        return;
      }
      // compute blkToDup
      synchronized (hlMap) {
        for (LocatedBlock lb : lBlocks.getLocatedBlocks()) {
          if (lb.getLocations().length < (s.getReplication() / 2 + 1)) {
            throw new IOException(
                "File " + s.getPath() + " doesn't have enough replicas! blk="
                    + lb.getBlock().getBlockId() + " expect=" + (
                    s.getReplication() / 2 + 1) + " actual=" + lb
                    .getLocations().length);
          }
          for (DatanodeInfo di : lb.getLocations()) {
            LinkedList<BlocksToDup> hlList = hlMap.get(di);
            BlocksToDup btd;
            if (hlList == null) {
              hlList = new LinkedList<>();
              btd = new BlocksToDup(DST_POOL);
              hlList.addFirst(btd);
              hlMap.put(di, hlList);
            } else {
              btd = hlList.getFirst();
              if (btd.size() >= SPLIT_THRESHOLD) {
                btd = new BlocksToDup(info.getDstPoolId());
                hlList.addFirst(btd);
              }
            }
            Block srcBlock = lb.getBlock().getLocalBlock();
            Block dstBlock = blkMap.get(srcBlock.getBlockId());
            btd.addDupBlock(srcBlock.getBlockId(), dstBlock.getBlockId(),
                srcBlock.getNumBytes(), srcBlock.getGenerationStamp(),
                dstBlock.getGenerationStamp());
          }
        }
      }
      // compute blkToLink
      for (LocatedBlock lb : lBlocks.getLocatedBlocks()) {
        long srcBlkId = lb.getBlock().getBlockId();
        BlockToLink btl = pendingBlocks.get(srcBlkId);
        if (btl == null) {
          synchronized (pendingBlocks) {
            btl = pendingBlocks.get(srcBlkId);
            if (btl == null) {
              btl = new BlockToLink(srcBlkId, blkMap.get(srcBlkId).getBlockId(),
                  s.getReplication(), (s.getReplication() / 2) + 1);
              pendingBlocks.put(lb.getBlock().getBlockId(), btl);
            }
          }
        }
      }
    }

    void checkAllHardlinkDone() throws IOException {
      boolean allLinkDone = true;
      StringBuilder sbuilder = null;
      if (!pendingBlocks.isEmpty()) {
        DistributedFileSystem dfs =
            (DistributedFileSystem) FileSystem.get(info.dst, conf);
        sbuilder = new StringBuilder("Unfinished blocks{\n");
        for (BlockToLink btl : pendingBlocks.values()) {
          int rep =
              getRealReplicaSize(dfs, BLOCK_FILE_PREFIX + btl.dstBlkId, conf);
          if (rep < btl.expected) {
            allLinkDone = false;
            sbuilder.append(
                "blkId:" + btl.srcBlkId + "->" + btl.dstBlkId + " (rep:"
                    + btl.replication + " exp:" + btl.expected + " link:"
                    + btl.linked + "\n");
          }
        }
        sbuilder.append("}");
      }
      if (!allLinkDone) {
        throw new IOException(sbuilder.toString());
      }
    }
  }

  static int getRealReplicaSize(DistributedFileSystem dfs, String blkId,
      Configuration conf) {
    int rep = 0;
    try {
      byte[] res = getLocationFromBlockId(dfs, blkId, conf);
      BufferedReader input = new BufferedReader(
          new InputStreamReader(new ByteArrayInputStream(res), "UTF-8"));
      String line;
      while ((line = input.readLine()) != null) {
        if (line.startsWith("Block replica on datanode/rack")) {
          rep++;
        }
      }
    } catch (IOException e) {
      LOG.warn("Compute replica size of " + blkId + " " + dfs + " failed.", e);
    }
    return rep;
  }

  private static byte[] getLocationFromBlockId(DistributedFileSystem dfs,
      String blkId, Configuration conf) throws IOException {
    URI activeURI = DFSUtil.getInfoServer(HAUtil.getAddressOfActive(dfs), conf,
        DFSUtil.getHttpClientScheme(conf));
    StringBuilder url = new StringBuilder();
    url.append(activeURI.toString());
    url.append("/fsck?ugi=")
        .append(UserGroupInformation.getCurrentUser().getShortUserName());
    url.append("&blockId=").append(URLEncoder.encode(blkId, "UTF-8"));

    URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    URLConnection con;
    try {
      con = connectionFactory.openConnection(new URL(url.toString()),
          UserGroupInformation.isSecurityEnabled());
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
    InputStream stream = con.getInputStream();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = stream.read(buffer)) > 0) {
        bao.write(buffer, 0, len);
      }
    } finally {
      stream.close();
    }
    return bao.toByteArray();
  }

  /**
   * Rpc call to Datanode to do the replica hard link.
   */
  class LinkTask implements Callable<Block[]> {
    private DatanodeInfo dnInfo;
    private LinkedList<BlocksToDup> units;

    public LinkTask(DatanodeInfo dnInfo, LinkedList<BlocksToDup> cell) {
      this.dnInfo = dnInfo;
      this.units = cell;
    }

    @Override
    public Block[] call() throws Exception {
      Thread.currentThread().setName(dnInfo.getIpcAddr(true));
      InetSocketAddress dnAddr =
          NetUtils.createSocketAddr(dnInfo.getIpcAddr(true));
      FederationClientDatanodeProtocol fcdp =
          FederationClientDatanodeProtocolTranslatorPB
              .createFederationClientDatanodeProtocolProxy(dnAddr,
                  UserGroupInformation.getLoginUser(), conf);
      List<Block> success = new ArrayList<>();
      BlocksToDup blkToDup;
      while (units.size() > 0) {
        synchronized (units) {
          if (units.size() > 0) {
            blkToDup = units.removeFirst();
          } else {
            break;
          }
        }
        long time = Time.now();
        success.addAll(Arrays
            .asList(fcdp.addBlocksToNewPool(info.getSrcPoolId(), blkToDup)));
        time = Time.now() - time;
        if (time > SLOW_LINK_THRESHOLD) {
          LOG.info("Slow Hardlink! Link " + blkToDup.size() + " blocks to "
              + dnInfo.getIpcAddr(true) + " costs " + time + "ms");
        }
      }
      return success.toArray(new Block[0]);
    }
  }

  class BlockToLink {
    long srcBlkId;
    long dstBlkId;
    int replication;
    int expected;
    int linked;

    public BlockToLink(long srcBlkId, long dstBlkId, int rep, int expected) {
      this.srcBlkId = srcBlkId;
      this.dstBlkId = dstBlkId;
      this.replication = rep;
      this.expected = expected;
      this.linked = 0;
    }
  }

  private String absolute(FileStatus s) {
    return s.getPath().toUri().getPath();
  }
}
