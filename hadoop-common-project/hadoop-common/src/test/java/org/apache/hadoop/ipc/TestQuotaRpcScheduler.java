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
package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.Mockito;
import static org.apache.hadoop.ipc.QuotaRpcScheduler.HDFS_NAMENODE_RPC_QUOTA_KEY;
import static org.apache.hadoop.ipc.QuotaRpcScheduler.HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestQuotaRpcScheduler {

  @Test
  public void testBackoff() throws Exception {
    final String user = "u_backoff";
    final String methodName = "addBlock";
    final long quotaLimit = 10;
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(ugi.getShortUserName()).thenReturn(user);

    Configuration conf = new Configuration();
    conf.set(HDFS_NAMENODE_RPC_QUOTA_KEY,
        user + HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT + methodName
            + HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT + quotaLimit);

    QuotaRpcScheduler scheduler = new QuotaRpcScheduler(1, "ns", conf);
    Schedulable mockCall = Mockito.mock(Schedulable.class);
    Mockito.when(mockCall.getUserGroupInformation()).thenReturn(ugi);
    Mockito.when(mockCall.getMethodName()).thenReturn(methodName);
    for (int i = 0; i < 10; i++) {
      assertFalse(scheduler.shouldBackOff(mockCall));
    }
    // Test backoff.
    assertTrue(scheduler.shouldBackOff(mockCall));
    // Test refill.
    Thread.sleep(1000);
    assertFalse(scheduler.shouldBackOff(mockCall));
  }
}
