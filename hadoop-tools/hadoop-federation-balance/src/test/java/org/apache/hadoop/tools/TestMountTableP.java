package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.procedure.MountTableProcedure;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestMountTableP {
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static Configuration routerConf;
  private static List<MountTable> mockMountTable;
  private static StateStoreService stateStore;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with State Store + admin + RPC
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    // Add two name services for testing
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(createNamenodeReport("ns0", "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    membership.registerNamenode(createNamenodeReport("ns1", "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(routerContext);
  }

  @Before
  public void testSetup() throws Exception {
    assertTrue(
        synchronizeRecords(stateStore, mockMountTable, MountTable.class));
    // Avoid running with random users
    routerContext.resetAdminClient();
  }

  @Test
  public void testUpdateMountpoint() throws Exception {
    // Firstly add mount entry: /test-path->{ns0,/test-path}.
    String fedPath = "/test-path";
    String dst = "/test-dst";
    MountTable newEntry = MountTable
        .newInstance(fedPath, Collections.singletonMap("ns0", fedPath),
            Time.now(), Time.now());
    MountTableManager mountTable =
        routerContext.getAdminClient().getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());
    // verify the mount entry is added successfully.
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance("/");
    stateStore.loadCache(MountTableStoreImpl.class, true); // load cache.
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    assertEquals(3, response.getEntries().size());

    // test SingleMountTableProcedure updates the mount point.
    String dstNs = "ns1";
    MountTableProcedure smtp =
        new MountTableProcedure("single-mount-table-procedure", null,
            1000, fedPath, dst, dstNs, routerConf);
    assertTrue(smtp.execute());
    stateStore.loadCache(MountTableStoreImpl.class, true); // load cache.
    // verify the mount entry is updated to /
    MountTable entry =
        MountTableProcedure.getMountEntry(fedPath, mountTable);
    assertNotNull(entry);
    assertEquals(1, entry.getDestinations().size());
    String nsId = entry.getDestinations().get(0).getNameserviceId();
    String dstPath = entry.getDestinations().get(0).getDest();
    assertEquals(dstNs, nsId);
    assertEquals(dst, dstPath);
  }

  @Test
  public void testSeDe() throws Exception {
    String fedPath = "/test-path";
    String dst = "/test-dst";
    String dstNs = "ns1";
    MountTableProcedure smtp =
        new MountTableProcedure("single-mount-table-procedure", null,
            1000, fedPath, dst, dstNs, routerConf);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutput dataOut = new DataOutputStream(bao);
    smtp.write(dataOut);
    smtp = new MountTableProcedure();
    smtp.readFields(
        new DataInputStream(new ByteArrayInputStream(bao.toByteArray())));
//    assertEquals(fedPath, smtp.getFedPath());
//    assertEquals(dst, smtp.getDstPath());
//    assertEquals(dstNs, smtp.getDstNs());
  }
}
