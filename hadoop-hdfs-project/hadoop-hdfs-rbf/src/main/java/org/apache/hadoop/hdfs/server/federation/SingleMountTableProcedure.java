package org.apache.hadoop.hdfs.server.federation;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Update the mount table:
 * Old mount table:
 *   /a/b/c -> {ns:src path:/a/b/c}
 * New mount table:
 *   /a/b/c -> {ns:dst path:/a/b/c}
 */
public class SingleMountTableProcedure extends Procedure {

  private String fedPath;
  private String dstPath;
  private String dstNs;
  private Configuration conf;
  private RouterClient rClient;

  public SingleMountTableProcedure() {}

  /**
   * Update mount entry fedPath to specified dst uri.
   *
   * @param fedPath the federation path to be updated.
   * @param dst the sub-cluster uri of the dst path.
   * @param conf the configuration.
   */
  public SingleMountTableProcedure(String fedPath, String dstPath, String dstNs,
      Configuration conf) {
    this.fedPath = fedPath;
    this.dstPath = dstPath;
    this.dstNs = dstNs;
    this.conf = conf;
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
    rClient = new RouterClient(routerSocket, conf);
    MountTableManager mountTable = rClient.getMountTableManager();

    MountTable originalEntry = getMountEntry(fedPath, mountTable);
    if (originalEntry == null) {
      throw new RuntimeException("Mount table " + fedPath + " doesn't exist");
    } else {
      RemoteLocation remoteLocation =
          new RemoteLocation(dstNs, dstPath, fedPath);
      originalEntry.setDestinations(Arrays.asList(remoteLocation));
      UpdateMountTableEntryRequest updateRequest =
          UpdateMountTableEntryRequest.newInstance(originalEntry);
      UpdateMountTableEntryResponse response =
          mountTable.updateMountTableEntry(updateRequest);
      if (!response.getStatus()) {
        throw new RuntimeException("Failed update mount table " + fedPath);
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
  public static MountTable getMountEntry(String mount,
      MountTableManager mountTable)
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
    Text.writeString(out, fedPath);
    Text.writeString(out, dstNs);
    conf.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fedPath = Text.readString(in);
    dstNs = Text.readString(in);
    conf = new Configuration(false);
    conf.readFields(in);
  }
}
