package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import org.apache.hadoop.hdfs.server.federation.SingleMountTableProcedure;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.procedure.Job;
import org.apache.hadoop.hdfs.server.namenode.procedure.ProcedureScheduler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Move mount entry and data to a new sub-cluster using distcp.
 */
public class DistCpFedBalance extends Configured implements Tool {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpFedBalance.class);

  public DistCpFedBalance(Configuration conf) {
    super(conf);
  }

  /**
   * Usage:
   *
   */
  @Override
  public int run(String[] args) throws Exception {
    String fedPath = args[0];
    Path src = getSrcPath(fedPath);
    Path dst = new Path(args[1]);
    FedBalanceContext context = new FedBalanceContext(src, dst, getConf());

    ProcedureScheduler scheduler = new ProcedureScheduler(getConf());
    scheduler.init();
    try {
      DistCpProcedure dcp =
          new DistCpProcedure("distcp-procedure", null, 1000, context);
      SingleMountTableProcedure smtp =
          new SingleMountTableProcedure(fedPath, dst.toUri().getPath(),
              dst.toUri().getAuthority(), getConf());
      Job balanceJob =
          new Job.Builder<>().nextProcedure(dcp).nextProcedure(smtp).build();
      scheduler.submit(balanceJob);
      scheduler.waitUntilDone(balanceJob);
    } catch (IOException e) {
      LOG.error("Balance job failed.", e);
      return -1;
    }
    return 0;
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
      return new Path("hdfs://" + ns, path);
    }
  }

  public static void main(String[] argv) throws Exception {
    Configuration conf = new HdfsConfiguration();
    DistCpFedBalance balance = new DistCpFedBalance(conf);

    int res = ToolRunner.run(balance, argv);
    System.exit(res);
  }
}
