package org.apache.hadoop.tools;

public class FedBalanceConfigs {
  public static final String DISTCP_PROCEDURE_MAP_NUM =
      "distcp.procedure.map.num";
  public static final int DISTCP_PROCEDURE_MAP_NUM_DEFAULT = 10;
  public static final String DISTCP_PROCEDURE_BAND_WIDTH_LIMIT =
      "distcp.procedure.bandwidth.limit";
  public static final int DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT = 1;
  public static final String CURRENT_SNAPSHOT_NAME = "DISTCP-BALANCE-CURRENT";
  public static final String NEXT_SNAPSHOT_NAME = "DISTCP-BALANCE-NEXT";
}
