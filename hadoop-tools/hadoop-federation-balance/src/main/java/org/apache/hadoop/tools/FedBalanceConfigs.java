package org.apache.hadoop.tools;

/** Federation balance configuration properties */
public interface FedBalanceConfigs {
  /* The class used for federation balance */
  String FEDERATION_BALANCE_CLASS = "federation.balance.class";
  /* The map number of distcp job */
  String DISTCP_PROCEDURE_MAP_NUM = "distcp.procedure.map.num";
  int DISTCP_PROCEDURE_MAP_NUM_DEFAULT = 10;
  /* The bandwidth limit of distcp job */
  String DISTCP_PROCEDURE_BAND_WIDTH_LIMIT = "distcp.procedure.bandwidth.limit";
  int DISTCP_PROCEDURE_BAND_WIDTH_LIMIT_DEFAULT = 1;
  String LAST_SNAPSHOT_NAME = "DISTCP-BALANCE-CURRENT";
  String CURRENT_SNAPSHOT_NAME = "DISTCP-BALANCE-NEXT";
  /* When there is no diff between source path and target path but there are
     open files in source path, force close all the open files */
  String DISTCP_PROCEDURE_FORCE_CLOSE_OPEN_FILES =
      "distcp.procedure.force.close.open.files";
  /* Move source path to trash after all the data are sync to target */
  String DISTCP_PROCEDURE_MOVE_TO_TRASH = "distcp.procedure.move.to.trash";
  boolean DISTCP_PROCEDURE_MOVE_TO_TRASH_DEFAULT = true;
}
