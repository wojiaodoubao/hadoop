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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
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

import static org.apache.hadoop.tools.DistCpBalanceOptions.ROUTER;
import static org.apache.hadoop.tools.DistCpBalanceOptions.FORCE_CLOSE_OPEN;
import static org.apache.hadoop.tools.DistCpBalanceOptions.MAP;
import static org.apache.hadoop.tools.DistCpBalanceOptions.BANDWIDTH;
import static org.apache.hadoop.tools.DistCpBalanceOptions.MOVE_TO_TRASH;
import static org.apache.hadoop.tools.DistCpBalanceOptions.cliOptions;

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

  public DistCpFedBalance() {
    super();
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    CommandLine command = parser.parse(DistCpBalanceOptions.cliOptions, args, true);
    String[] leftOverArgs = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      printUsage();
      return -1;
    }
    String cmd = leftOverArgs[0];
    if (cmd.equals("submit")) {
      if (leftOverArgs.length < 3) {
        printUsage();
        return -1;
      }
      String inputSrc = leftOverArgs[1];
      String inputDst = leftOverArgs[2];
      return submit(command, inputSrc, inputDst);
    } else if (cmd.equals("continue")) {
      return continueJob();
    } else {
      printUsage();
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
  private int submit(CommandLine command, String inputSrc, String inputDst) throws IOException {
    // parse options.
    boolean routerCluster = true;
    boolean forceCloseOpen = false;
    int map = 10;
    int bandWidthLimit = 1;
    boolean moveToTrash = true;

    if (command.hasOption(ROUTER.getOpt())) {
      routerCluster =
          command.getOptionValue(ROUTER.getOpt()).equalsIgnoreCase("true");
    }
    if (command.hasOption(FORCE_CLOSE_OPEN.getOpt())) {
      forceCloseOpen = command.getOptionValue(FORCE_CLOSE_OPEN.getOpt())
          .equalsIgnoreCase("true");
    }
    if (command.hasOption(MAP.getOpt())) {
      map = Integer.parseInt(command.getOptionValue(MAP.getOpt()));
    }
    if (command.hasOption(BANDWIDTH.getOpt())) {
      bandWidthLimit =
          Integer.parseInt(command.getOptionValue(BANDWIDTH.getOpt()));
    }
    if (command.hasOption(MOVE_TO_TRASH.getOpt())) {
      moveToTrash = command.getOptionValue(MOVE_TO_TRASH.getOpt())
          .equalsIgnoreCase("true");
    }

    // Submit the job.
    BalanceProcedureScheduler scheduler =
        new BalanceProcedureScheduler(getConf());
    scheduler.init(false);
    try {
      BalanceJob balanceJob =
          constructBalanceJob(inputSrc, inputDst, routerCluster, forceCloseOpen,
              map, bandWidthLimit, moveToTrash);
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
      boolean routerCluster, boolean forceCloseOpen, int map, int bandwidth,
      boolean moveToTrash) throws IOException {
    // Construct job context.
    FedBalanceContext context;
    Path dst = new Path(inputDst);
    if (dst.toUri().getAuthority() == null) {
      throw new IOException("The destination cluster must be specified.");
    }
    if (routerCluster) { // router-based federation.
      Path src = getSrcPath(inputSrc);
      String mount = inputSrc;
      context =
          new FedBalanceContext(src, dst, mount, getConf(), forceCloseOpen,
              routerCluster, map, bandwidth, moveToTrash);
    } else { // normal federation cluster.
      Path src = new Path(inputSrc);
      if (src.toUri().getAuthority() == null) {
        throw new IOException("The source cluster must be specified.");
      }
      context =
          new FedBalanceContext(src, dst, "no-mount", getConf(), forceCloseOpen,
              routerCluster, map, bandwidth, moveToTrash);
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

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        "fedbalance OPTIONS [submit|continue] <src> <target>\n\nOPTIONS",
        cliOptions);
  }
}
