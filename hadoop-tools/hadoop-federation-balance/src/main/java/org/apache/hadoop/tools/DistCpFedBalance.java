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
package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.procedure.BalanceProcedure;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.procedure.BalanceJob;
import org.apache.hadoop.hdfs.procedure.BalanceProcedureScheduler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Balance data from src cluster to dst cluster with distcp.
 *
 * 1. Move data from the source path to the destination path with distcp.
 * 2. Update the the mount entry.
 * 3. Delete the source path to trash.
 */
public class DistCpFedBalance extends Configured implements Tool {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpFedBalance.class);
  private static final String USAGE = "usage: fedbalance:\n"
      + "\t[submit [OPTIONS] <mount_point|source_path> <target_path>]\n"
      + "\t\tOPTIONS is none or any of:\n"
      + "\t\t-router [true|false]\n"
      + "\t\t-forceCloseOpen [true|false]\n"
      + "\t[continue]";

  public DistCpFedBalance() {
    super();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length < 1) {
      System.out.println(USAGE);
      return -1;
    }
    int index = 0;
    String command = args[index++];
    if (command.equals("submit")) {
      return submit(args, index);
    } else if (command.equals("continue")) {
      return continueJob();
    } else {
      System.out.println(USAGE);
      return -1;
    }
  }

  /**
   * Recover and continue the unfinished jobs.
   */
  private int continueJob() throws InterruptedException {
    BalanceProcedureScheduler scheduler =
        new BalanceProcedureScheduler(getConf());
    try {
      scheduler.init(true);
      while (true) {
        Collection<BalanceJob> jobs = scheduler.getAllJobs();
        int unfinished = 0;
        for (BalanceJob job : jobs) {
          if (!job.isJobDone()) {
            unfinished++;
          }
          System.out.println(job);
        }
        if (unfinished == 0) {
          break;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }
    } catch (IOException e) {
      LOG.error("Continue balance job failed.", e);
      return -1;
    } finally {
      scheduler.shutDown();
    }
    return 0;
  }

  /**
   * Start a ProcedureScheduler and submit the job.
   */
  private int submit(String[] args, int index) throws IOException {
    if (args.length < 3) {
      System.out.println(USAGE);
      return -1;
    }
    // parse options.
    boolean routerCluster = true;
    boolean forceCloseOpen = false;
    while (args[index].startsWith("-")) {
      String option = args[index++];
      if (option.equals("-router")) {
        routerCluster = args[index++].equalsIgnoreCase("true");
      } else if (option.equals("-forceCloseOpen")) {
        forceCloseOpen = args[index++].equalsIgnoreCase("true");
      } else {
        System.out.println(USAGE);
        return -1;
      }
    }
    String inputSrc = args[index++];
    String inputDst = args[index++];

    // Submit the job.
    BalanceProcedureScheduler scheduler =
        new BalanceProcedureScheduler(getConf());
    scheduler.init(false);
    try {
      BalanceJob balanceJob =
          constructBalanceJob(inputSrc, inputDst, routerCluster, forceCloseOpen);
      // Submit and wait until the job is done.
      scheduler.submit(balanceJob);
      scheduler.waitUntilDone(balanceJob);
    } catch (IOException e) {
      LOG.error("Submit balance job failed.", e);
      return -1;
    } finally {
      scheduler.shutDown();
    }
    return 0;
  }

  /**
   * Construct the balance job.
   */
  private BalanceJob constructBalanceJob(String inputSrc, String inputDst,
      boolean routerCluster, boolean forceCloseOpen) throws IOException {
    // Construct job context.
    FedBalanceContext context;
    Path dst = new Path(inputDst);
    if (dst.toUri().getAuthority() == null) {
      throw new IOException("The destination cluster must be specified.");
    }
    if (routerCluster) {
      Path src = getSrcPath(inputSrc);
      String mount = inputSrc;
      context =
          new FedBalanceContext(src, dst, mount, getConf(), forceCloseOpen,
              routerCluster);
    } else {
      Path src = new Path(inputSrc);
      if (src.toUri().getAuthority() == null) {
        throw new IOException("The source cluster must be specified.");
      }
      context =
          new FedBalanceContext(src, dst, "no-mount", getConf(), forceCloseOpen,
              routerCluster);
    }

    // Construct the balance job.
    BalanceJob.Builder<BalanceProcedure> builder = new BalanceJob.Builder<>();
    DistCpProcedure dcp =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    builder.nextProcedure(dcp);
    if (routerCluster) {
      MountTableProcedure mtp =
          new MountTableProcedure("mount-table-procedure", null, 1000, inputSrc,
              dst.toUri().getPath(), dst.toUri().getAuthority(), getConf());
      builder.nextProcedure(mtp);
    }
    TrashProcedure tp =
        new TrashProcedure("trash-procedure", null, 1000, context);
    builder.nextProcedure(tp);
    return builder.build();
  }

  /**
   * Get src uri from Router.
   */
  private Path getSrcPath(String fedPath) throws IOException {
    String address = getConf().getTrimmed(
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
    RouterClient rClient = new RouterClient(routerSocket, getConf());
    MountTableManager mountTable = rClient.getMountTableManager();
    MountTable entry =
        MountTableProcedure.getMountEntry(fedPath, mountTable);
    if (entry == null) {
      throw new IllegalArgumentException(
          "The mount point doesn't exist. path=" + fedPath);
    } else if (entry.getDestinations().size() > 1) {
      throw new IllegalArgumentException(
          "The mount point has more than one destination. path=" + fedPath);
    } else {
      String ns = entry.getDestinations().get(0).getNameserviceId();
      String path = entry.getDestinations().get(0).getDest();
      return new Path("hdfs://" + ns + path);
    }
  }
}
