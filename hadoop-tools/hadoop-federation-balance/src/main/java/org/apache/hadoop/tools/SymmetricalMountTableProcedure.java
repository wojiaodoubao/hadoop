package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.procedure.Procedure;
import org.apache.hadoop.net.NetUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Update the mount table:
 * Old mount table:
 *   /a/b/c -> {ns:src path:/a/b/c}
 * New mount table:
 *   /a/b/c -> {ns:dst path:/a/b/c}
 */
public class SymmetricalMountTableProcedure extends Procedure {

  private FedBalanceContext context;
  private Configuration conf;

  public SymmetricalMountTableProcedure() {}

  public SymmetricalMountTableProcedure(FedBalanceContext context) {
    this.context = context;
    this.conf = context.getConf();
  }

  @Override
  public boolean execute(Procedure lastProcedure)
      throws RetryException, IOException {
    updateMountTable();
    return true;
  }

  private void updateMountTable() throws IOException {
    String address = conf.getTrimmed(
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
    RouterClient rClient = new RouterClient(routerSocket, conf);
    MountTableManager mountTable = rClient.getMountTableManager();

    String fPath = context.getFederationPath();
    MountTable originalEntry = getMountEntry(fPath, mountTable);
    if (originalEntry == null) {
      throw new RuntimeException("Mount table " + fPath + " doesn't exist");
    } else {
      String dstNs = context.getDst().toUri().getAuthority();
      RemoteLocation remoteLocation = new RemoteLocation(dstNs, fPath, fPath);
      originalEntry.setDestinations(
          Collections.list(new Arrays.asList(remoteLocation)));
      UpdateMountTableEntryRequest updateRequest =
          UpdateMountTableEntryRequest.newInstance(originalEntry);
      UpdateMountTableEntryResponse response =
          mountTable.updateMountTableEntry(updateRequest)
      if (!response.getStatus()) {
        throw new RuntimeException("Failed update mount table " + fPath);
      }
    }
  }

  /**
   * Gets the mount table entry.
   * @param mount name of the mount entry.
   * @param mountTable the mount table.
   * @return corresponding mount entry.
   * @throws IOException in case of failure to retrieve mount entry.
   */
  private MountTable getMountEntry(String mount, MountTableManager mountTable)
      throws IOException {
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mount);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> results = getResponse.getEntries();
    MountTable existingEntry = null;
    for (MountTable result : results) {
      if (mount.equals(result.getSourcePath())) {
        existingEntry = result;
      }
    }
    return existingEntry;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    context.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    context = new FedBalanceContext();
    context.readFields(in);
    conf = context.getConf();
  }
}
