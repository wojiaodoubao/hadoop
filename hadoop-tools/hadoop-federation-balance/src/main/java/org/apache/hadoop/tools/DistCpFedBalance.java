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

/**
 * Move mount entry and data to a new sub-cluster using distcp.
 */
public class DistCpFedBalance extends Configured implements Tool {

  public static final Logger LOG =
      LoggerFactory.getLogger(DistCpFedBalance.class);

  public DistCpFedBalance() {
    super();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("usage: fedbalance [source_mount_point] [target_path]");
    }
    String fedPath = args[0];
    Path src = getSrcPath(fedPath);
    Path dst = new Path(args[1]);
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
              1000, fedPath, dst.toUri().getPath(), dst.toUri().getAuthority(),
              getConf());
      TrashProcedure tp = new TrashProcedure("trash-procedure", null, 1000, context);
      Job balanceJob = new Job.Builder<>().nextProcedure(dcp)
          .nextProcedure(smtp).nextProcedure(tp).build();
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
