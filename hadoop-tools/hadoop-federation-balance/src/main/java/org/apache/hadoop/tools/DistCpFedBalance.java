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
import org.apache.hadoop.hdfs.server.federation.SingleMountTableProcedure;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureScheduler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Move mount entry and data to a new sub-cluster using distcp.
 */
public class DistCpFedBalance extends Configured implements Tool {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpFedBalance.class);
  private static final String USAGE = "usage: fedbalance:\n"
            + "\t[submit <source_mount_point> <target_path>]\n"
            + "\t[continue]";

  public DistCpFedBalance() {
    super();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length < 1) {
      System.out.println(USAGE);
    }
    int index = 0;
    String command = args[index++];
    if (command.equals("submit")) {
      String fedPath = args[index++];
      Path src = getSrcPath(fedPath);
      Path dst = new Path(args[index++]);
      if (dst.toUri().getAuthority() == null) {
        throw new IOException("The destination cluster must be specified.");
      }
      FedBalanceContext context = new FedBalanceContext(src, dst, getConf());

      ProcedureScheduler scheduler = new ProcedureScheduler(getConf());
      scheduler.init();
      try {
        DistCpProcedure dcp =
            new DistCpProcedure("distcp-procedure", null, 1000, context);
        SingleMountTableProcedure smtp =
            new SingleMountTableProcedure("single-mount-table-procedure", null,
                1000, fedPath, dst.toUri().getPath(),
                dst.toUri().getAuthority(), getConf());
        TrashProcedure tp =
            new TrashProcedure("trash-procedure", null, 1000, context);
        Job balanceJob =
            new Job.Builder<>().nextProcedure(dcp).nextProcedure(smtp)
                .nextProcedure(tp).build();
        scheduler.submit(balanceJob);
        scheduler.waitUntilDone(balanceJob);
      } catch (IOException e) {
        LOG.error("Balance job failed.", e);
        return -1;
      }
    } else if (command.equals("continue")) {
      ProcedureScheduler scheduler = new ProcedureScheduler(getConf());
      scheduler.init();
      while (true) {
        Collection<Job> jobs = scheduler.getAllJobs();
        for (Job job : jobs) {
          System.out.println(job);
        }
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
      }
    } else {
      System.out.println(USAGE);
    }
    return 0;
  }

  /**
   * Get src uri from Router.
   */
  private Path getSrcPath(String fedPath) throws IOException,
      URISyntaxException {
    String address = getConf().getTrimmed(
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
    RouterClient rClient = new RouterClient(routerSocket, getConf());
    MountTableManager mountTable = rClient.getMountTableManager();
    MountTable entry =
        SingleMountTableProcedure.getMountEntry(fedPath, mountTable);
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
