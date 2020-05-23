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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Federation balance configuration properties.
 */
@InterfaceAudience.Private
public final class FedBalanceConfigs {
  /* The class used for federation balance */
  public static final String FEDERATION_BALANCE_CLASS =
      "federation.balance.class";
  /* The map number of distcp job */
  public static final String DISTCP_PROCEDURE_MAP_NUM =
      "distcp.procedure.map.num";
  public static final int DISTCP_PROCEDURE_MAP_NUM_DEFAULT = 10;
  /* The bandwidth limit of distcp job */
  public static final String DISTCP_PROCEDURE_BAND_WIDTH_LIMIT =
      "distcp.procedure.bandwidth.limit";
  public static final int DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT = 1;
  public static final String LAST_SNAPSHOT_NAME = "DISTCP-BALANCE-CURRENT";
  public static final String CURRENT_SNAPSHOT_NAME = "DISTCP-BALANCE-NEXT";
  /* Move source path to trash after all the data are sync to target */
  public static final String DISTCP_PROCEDURE_MOVE_TO_TRASH =
      "distcp.procedure.move.to.trash";
  public static final boolean DISTCP_PROCEDURE_MOVE_TO_TRASH_DEFAULT = true;

  private FedBalanceConfigs(){}
}
