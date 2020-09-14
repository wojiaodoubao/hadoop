/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.QuotaRpcScheduler;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.ipc.QuotaRpcScheduler.HDFS_NAMENODE_RPC_QUOTA_KEY;
import static org.apache.hadoop.ipc.QuotaRpcScheduler.HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.fail;

public class TestNameNodeRpcQuota {

  private MiniDFSCluster cluster;
  private int nnPort = 0;

  @Test
  public void testQuotaExceed() throws Exception {
    Configuration conf = new Configuration();
    String prefix = CommonConfigurationKeys.IPC_NAMESPACE + "." + nnPort + ".";
    conf.set(prefix + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY,
        LinkedBlockingQueue.class.getName());
    conf.set(prefix + CommonConfigurationKeys
        .IPC_SCHEDULER_IMPL_KEY, QuotaRpcScheduler.class.getName());

    final String user = "u_test_quota_exceed";
    final String methodName = "getFileInfo";
    final long quotaLimit = 10;
    conf.set(HDFS_NAMENODE_RPC_QUOTA_KEY,
        user + HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT + methodName
            + HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT + quotaLimit);
    conf.setBoolean(prefix + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);

    try {
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnPort).build();
      cluster.waitActive();
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      // Consume all quota.
      for (int i = 0; i < 10; i++) {
        doAs(ugi, ()->cluster.getFileSystem().getFileStatus(new Path("/")));
      }
      // Test quota exceed.
      doAs(ugi, () -> intercept(RemoteException.class,
          "Server too busy - disconnecting", "Expect too busy exception",
          () -> {
            cluster.getFileSystem().getFileStatus(new Path("/"));
            return null;
          }));
      // Test quota refill.
      Thread.sleep(1000);
      for (int i = 0; i < 10; i++) {
        doAs(ugi, ()->cluster.getFileSystem().getFileStatus(new Path("/")));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void doAs(UserGroupInformation ugi, Callable callable) {
    ugi.doAs((PrivilegedAction) () -> {
      try {
        callable.call();
      } catch (Exception e) {
        fail("Unexpected exception " + e.getMessage());
      }
      return null;
    });
  }
}
