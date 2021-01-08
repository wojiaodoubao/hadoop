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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;
import org.apache.hadoop.tools.fedbalance.FedBalanceConfigs;
import org.apache.hadoop.tools.fedbalance.FedBalanceContext;
import org.apache.hadoop.tools.fedbalance.TrashProcedure;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceJob;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedureScheduler;

import java.io.IOException;
import java.util.List;

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
public class RouterFederationRename {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterFederationRename.class.getName());
  private final RouterRpcServer rpcServer;
  private final Configuration conf;

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
    if (!rpcServer.enableRenameAcrossNamespace()) {
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
    if (srcLoc.getNameserviceId().equals(dstLoc.getNameserviceId())) {
      throw new IOException(
          "Rename of " + src + " to " + dst + " are at the same namespace.");
    }
    // Build and submit router federation rename job.
    BalanceJob job = buildRouterRenameJob(
        new Path("hdfs://" + srcLoc.getNameserviceId() + srcLoc.getDest()),
        new Path("hdfs://" + dstLoc.getNameserviceId() + dstLoc.getDest()));
    BalanceProcedureScheduler scheduler = rpcServer.getFedRenameScheduler();
    scheduler.submit(job);
    LOG.info("Rename {} to {} from namespace {} to {}. JobId={}.", src, dst,
        srcLoc.getNameserviceId(), dstLoc.getNameserviceId(), job.getId());
    scheduler.waitUntilDone(job);
    if (job.getError() != null) {
      throw new IOException("Rename of " + src + " to " + dst + " failed.",
          job.getError());
    }
    return true;
  }

  /**
   * Build router federation rename job moving data from src to dst.
   */
  private BalanceJob buildRouterRenameJob(Path src, Path dst)
      throws IOException {
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
    if (map < 0 || bandwidth < 0 || delay < 0 || diff < 0) {
      throw new IOException("Unexpected negative value. map=" + map
          + " bandwidth=" + bandwidth + " delay=" + delay + " diff=" + diff);
    }
    // Construct job context.
    FedBalanceContext context =
        new FedBalanceContext.Builder(src, dst, NO_MOUNT, conf)
            .setForceCloseOpenFiles(forceCloseOpen).setUseMountReadOnly(true)
            .setMapNum(map).setBandwidthLimit(bandwidth).setTrash(trashOpt)
            .setDelayDuration(delay).setDiffThreshold(diff)
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
}
