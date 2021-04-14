/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.RouterINode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;
import org.apache.hadoop.tools.fedbalance.FedBalanceConfigs;
import org.apache.hadoop.tools.fedbalance.FedBalanceContext;
import org.apache.hadoop.tools.fedbalance.TrashProcedure;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceJob;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedureScheduler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_MAP;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_DELAY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_DIFF;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_DIFF_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_TRASH;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_TRASH_DEFAULT;
import static org.apache.hadoop.tools.fedbalance.FedBalance.DISTCP_PROCEDURE;
import static org.apache.hadoop.tools.fedbalance.FedBalance.TRASH_PROCEDURE;
import static org.apache.hadoop.tools.fedbalance.FedBalance.NO_MOUNT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rename across router based federation namespaces.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RouterFederationRename {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterFederationRename.class.getName());
  private final RouterRpcServer rpcServer;
  private final Configuration conf;
  private final AtomicInteger routerRenameCounter = new AtomicInteger();
  public enum RouterRenameOption {
    NONE, DISTCP
  }

  public RouterFederationRename(RouterRpcServer rpcServer, Configuration conf) {
    this.rpcServer = rpcServer;
    this.conf = conf;
  }

  /**
   * Router federation rename across namespaces.
   *
   * @param src the source path. There is no mount point under the src path.
   * @param dst the dst path.
   * @param srcLocations the remote locations of src.
   * @param dstLocations the remote locations of dst.
   * @throws IOException if rename fails.
   * @return true if rename succeeds.
   */
  boolean routerFedRename(final String src, final String dst,
      final List<RemoteLocation> srcLocations,
      final List<RemoteLocation> dstLocations) throws IOException {
    if (!rpcServer.isEnableRenameAcrossNamespace()) {
      throw new IOException("Rename of " + src + " to " + dst
          + " is not allowed, no eligible destination in the same namespace was"
          + " found");
    }
    if (srcLocations.size() != 1 || dstLocations.size() != 1) {
      throw new IOException("Rename of " + src + " to " + dst + " is not"
          + " allowed. The remote location should be exactly one.");
    }
    RemoteLocation srcLoc = srcLocations.get(0);
    RemoteLocation dstLoc = dstLocations.get(0);
    String remoteSrc = srcLoc.getDest();
    String remoteDst = dstLoc.getDest();
    checkRouterRenamePath(src, dst, remoteSrc, remoteDst);
    checkRenameSrcPermission(srcLoc.getNameserviceId(), remoteSrc);
    checkRenameDstPermission(dstLoc.getNameserviceId(), remoteDst);

    UserGroupInformation routerUser = UserGroupInformation.getLoginUser();

    try {
      // as router user with saveJournal and task submission privileges
      return routerUser.doAs((PrivilegedExceptionAction<Boolean>) () -> {
        // Build and submit router federation rename job.
        // Build and submit router federation rename job.
        BalanceJob job = buildRouterRenameJob(srcLoc.getNameserviceId(),
            dstLoc.getNameserviceId(), srcLoc.getDest(), dstLoc.getDest());
        BalanceProcedureScheduler scheduler = rpcServer.getFedRenameScheduler();
        countIncrement();
        try {
          scheduler.submit(job);
          LOG.info("Rename {} to {} from namespace {} to {}. JobId={}.", src,
              dst, srcLoc.getNameserviceId(), dstLoc.getNameserviceId(),
              job.getId());
          scheduler.waitUntilDone(job);
          if (job.getError() != null) {
            throw new IOException("Rename of " + src + " to " + dst +
                " failed.", job.getError());
          }
          return true;
        } finally {
          countDecrement();
        }
      });
    } catch (InterruptedException e) {
      LOG.warn("Fed balance job is interrupted.", e);
      throw new InterruptedIOException(e.getMessage());
    }
  }

  static void checkRouterRenamePath(String src, String dst, String remoteSrc,
      String remoteDst) throws IOException {
    if (remoteSrc.contains("/.snapshot/")) {
      throw new IOException(
          "Router federation rename can't rename snapshot path. src=" + src
              + " dest=" + remoteSrc);
    }
    if (remoteDst.contains("/.snapshot/")) {
      throw new IOException(
          "Router federation rename can't rename snapshot path. dst=" + dst
              + " dest=" + remoteDst);
    }
  }

  @VisibleForTesting
  void checkRenameSrcPermission(String srcNs, String src) throws IOException {
    // Check permission.
    RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
    if (!pc.isSuperUser()) {
      Path srcPath = new Path("hdfs://" + srcNs + src);
      FileSystem fs = srcPath.getFileSystem(conf);
      String[] components = src.split(Path.SEPARATOR);
      RouterINode[] inodes = new RouterINode[components.length];
      // construct inodes.
      Path path = new Path("/");
      for (int i = 0; i < components.length; i++) {
        INode parent = i == 0 ? null : inodes[i - 1];
        inodes[i] = constructINode(fs, path, parent);
        if (i < components.length-1) {
          path = new Path(path, components[i + 1]);
        }
      }
      pc.checkPermission(inodes, src, false, null, FsAction.WRITE);
    }
  }

  @VisibleForTesting
  void checkRenameDstPermission(String dstNs, String dst) throws IOException {
    // Check permission.
    RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
    if (!pc.isSuperUser()) {
      Path dstPath = new Path("hdfs://" + dstNs + dst);
      FileSystem fs = dstPath.getFileSystem(conf);
      if (fs.exists(dstPath)) {
        throw new AccessControlException(
            "The dst path of router federation rename already exists!");
      }
      String[] components = dst.split(Path.SEPARATOR);
      RouterINode[] inodes = new RouterINode[components.length];
      // construct inodes.
      Path path = new Path("/");
      for (int i = 0; i < components.length - 1; i++) {
        INode parent = i == 0 ? null : inodes[i - 1];
        inodes[i] = constructINode(fs, path, parent);
        if (i < components.length-1) {
          path = new Path(path, components[i + 1]);
        }
      }
      pc.checkPermission(inodes, dst, false, FsAction.WRITE, null);
    }
  }

  static RouterINode constructINode(FileSystem fs, Path path, INode parent)
      throws IOException {
    FileStatus status = fs.getFileStatus(path);
    AclStatus acl = fs.getAclStatus(path);
    return new RouterINode(parent, status, acl);
  }

  /**
   * Build router federation rename job moving data from src to dst.
   * @param srcNs the source namespace id.
   * @param dstNs the dst namespace id.
   * @param src the source path.
   * @param dst the dst path.
   */
  private BalanceJob buildRouterRenameJob(String srcNs, String dstNs,
      String src, String dst) throws IOException {
    checkConfiguration(conf);
    Path srcPath = new Path("hdfs://" + srcNs + src);
    Path dstPath = new Path("hdfs://" + dstNs + dst);
    boolean forceCloseOpen =
        conf.getBoolean(DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE,
            DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE_DEFAULT);
    int map = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_MAP, -1);
    int bandwidth = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH, -1);
    long delay = conf.getLong(DFS_ROUTER_FEDERATION_RENAME_DELAY,
        DFS_ROUTER_FEDERATION_RENAME_DELAY_DEFAULT);
    int diff = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_DIFF,
        DFS_ROUTER_FEDERATION_RENAME_DIFF_DEFAULT);
    String trashPolicy = conf.get(DFS_ROUTER_FEDERATION_RENAME_TRASH,
        DFS_ROUTER_FEDERATION_RENAME_TRASH_DEFAULT);
    FedBalanceConfigs.TrashOption trashOpt =
        FedBalanceConfigs.TrashOption.valueOf(trashPolicy.toUpperCase());
    // Construct job context.
    FedBalanceContext context =
        new FedBalanceContext.Builder(srcPath, dstPath, NO_MOUNT, conf)
            .setForceCloseOpenFiles(forceCloseOpen)
            .setUseMountReadOnly(true)
            .setMapNum(map)
            .setBandwidthLimit(bandwidth)
            .setTrash(trashOpt)
            .setDelayDuration(delay)
            .setDiffThreshold(diff)
            .build();

    LOG.info(context.toString());
    // Construct the balance job.
    BalanceJob.Builder<BalanceProcedure> builder = new BalanceJob.Builder<>();
    DistCpProcedure dcp =
        new DistCpProcedure(DISTCP_PROCEDURE, null, delay, context);
    builder.nextProcedure(dcp);
    TrashProcedure tp =
        new TrashProcedure(TRASH_PROCEDURE, null, delay, context);
    builder.nextProcedure(tp);
    return builder.build();
  }

  public int getRouterFederationRenameCount() {
    return routerRenameCounter.get();
  }

  void countIncrement() {
    routerRenameCounter.incrementAndGet();
  }

  void countDecrement() {
    routerRenameCounter.decrementAndGet();
  }

  static void checkConfiguration(Configuration conf) throws IOException {
    int map = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_MAP, -1);
    int bandwidth = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH, -1);
    long delay = conf.getLong(DFS_ROUTER_FEDERATION_RENAME_DELAY,
        DFS_ROUTER_FEDERATION_RENAME_DELAY_DEFAULT);
    int diff = conf.getInt(DFS_ROUTER_FEDERATION_RENAME_DIFF,
        DFS_ROUTER_FEDERATION_RENAME_DIFF_DEFAULT);
    if (map < 0) {
      throw new IOException("map=" + map + " is negative. Please check "
          + DFS_ROUTER_FEDERATION_RENAME_MAP);
    } else if (bandwidth < 0) {
      throw new IOException(
          "bandwidth=" + bandwidth + " is negative. Please check "
              + DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH);
    } else if (delay < 0) {
      throw new IOException("delay=" + delay + " is negative. Please check "
          + DFS_ROUTER_FEDERATION_RENAME_DELAY);
    } else if (diff < 0) {
      throw new IOException("diff=" + diff + " is negative. Please check "
          + DFS_ROUTER_FEDERATION_RENAME_DIFF);
    }
  }
}
