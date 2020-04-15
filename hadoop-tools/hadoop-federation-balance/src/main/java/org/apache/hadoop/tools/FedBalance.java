package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.tools.FedBalanceConfigs.FEDERATION_BALANCE_CLASS;

public class FedBalance {
  public static void main(String args[]) throws Exception {
    Configuration conf = new HdfsConfiguration();
    Class<Tool> balanceClazz = (Class<Tool>) conf
        .getClass(FEDERATION_BALANCE_CLASS, DistCpFedBalance.class);
    Tool balancer = ReflectionUtils.newInstance(balanceClazz, conf);
    int res = ToolRunner.run(balancer, args);
    System.exit(res);
  }
}
