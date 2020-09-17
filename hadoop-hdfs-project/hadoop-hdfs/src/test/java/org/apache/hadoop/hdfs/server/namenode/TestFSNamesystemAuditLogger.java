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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.FSNamesystemAuditLogger;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.FSNamesystemAuditLogger.AuditLogBuilder;

import static org.junit.Assert.assertEquals;

public class TestFSNamesystemAuditLogger {

  @Test
  public void testAuditLogBuilder() {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_REDUCE_LOGS, true);
    FSNamesystemAuditLogger logger = new FSNamesystemAuditLogger();
    logger.initialize(conf);
    AuditLogBuilder builder = logger.new AuditLogBuilder();
    builder.appendKV("k0", "v0").appendKV("k1", null)
        .appendKV("k2", "v20", ",", "v21");
    assertEquals("k0=v0\tk2=v20,v21", builder.toString());
  }
}
