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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import java.io.IOException;

/**
 * Utility functions for the Federation balance.
 */
public class FedBalanceUtils {

  private FedBalanceUtils() {
    // Util class should not be initialized.
  }

  static boolean setMountEntryReadOnly(String mount, Configuration conf)
      throws IOException {
    RouterAdmin admin = new RouterAdmin(conf);
    String[] parameters = new String[] {mount, "-readonly", "true"};
    return admin.updateMount(parameters, 0);
  }

  static boolean cancelMountEntryReadOnly(String mount, Configuration conf)
      throws IOException {
    RouterAdmin admin = new RouterAdmin(conf);
    String[] parameters = new String[] {mount, "-readonly", "false"};
    return admin.updateMount(parameters, 0);
  }
}
