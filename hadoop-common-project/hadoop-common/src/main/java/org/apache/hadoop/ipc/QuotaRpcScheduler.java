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
package org.apache.hadoop.ipc;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The quota RPC scheduler allows to check quota for rpc requests.
 */
public class QuotaRpcScheduler implements RpcScheduler {

  public static final String HDFS_NAMENODE_RPC_QUOTA_KEY =
      "hdfs.namenode.rpc.quota.key";
  public static final String HDFS_NAMENODE_RPC_QUOTA_SPLIT = ",";
  public static final String HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT = ":";
  public static final org.slf4j.Logger LOG = LoggerFactory
      .getLogger(QuotaRpcScheduler.class.getName());
  /**
   * Key of quota map.
   */
  private class QuotaKey {
    String userName;
    String methodName;

    public QuotaKey(String userName, String methodName) {
      this.userName = userName;
      this.methodName = methodName;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37).append(userName).append(methodName)
          .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }
      QuotaKey qk = (QuotaKey) obj;
      return new EqualsBuilder()
          .append(userName, qk.userName)
          .append(methodName, qk.methodName)
          .build();
    }
  }

  /**
   * The rpc request quota. The available quota is refilled to limit every
   * second. It is not a precise quota controller.
   */
  private class Quota {
    private long lastRefillTime = -1;
    private final long REFILL_INTERVAL = 1000;
    private long limit;
    private long available;

    /**
     * Construct rpc request quota.
     * @param limit the rpc limit per second.
     */
    public Quota(long limit) {
      this.limit = limit;
    }

    /**
     * Consume one quota.
     *
     * @return true if the quota is available. Otherwise false.
     */
    public synchronized boolean consume() {
      long now = Time.monotonicNow();
      if (now - lastRefillTime > REFILL_INTERVAL) {
        available = limit;
        lastRefillTime = now;
      }
      if (available > 0) {
        available--;
        return true;
      } else {
        return false;
      }
    }
  }

  private volatile Map<QuotaKey, Quota> quotaMap;

  /**
   * Construct QuotaRpcScheduler.
   */
  public QuotaRpcScheduler(int numLevels, String ns, Configuration conf) {
    loadQuotaMap(conf);
  }

  /**
   * Reload rpc quota from configuration file.
   */
  public void refreshQuota() {
    loadQuotaMap(new Configuration());
  }

  private void loadQuotaMap(Configuration conf) {
    Map<QuotaKey, Quota> newMap = new HashMap<>();
    String quotaStr = conf.get(HDFS_NAMENODE_RPC_QUOTA_KEY);
    if (quotaStr != null) {
      String[] items = quotaStr.split(HDFS_NAMENODE_RPC_QUOTA_SPLIT);
      for (String item : items) {
        String[] parts = item.split(HDFS_NAMENODE_RPC_QUOTA_PART_SPLIT);
        if (parts.length != 3) {
          continue;
        }
        String user = parts[0];
        String method = parts[1];
        long quotaLimit;
        try {
          quotaLimit = Long.parseLong(parts[2]);
        } catch (NumberFormatException nfe) {
          continue;
        }
        newMap.put(new QuotaKey(user, method), new Quota(quotaLimit));
        LOG.info("Load quota " + user + ":" + method + ":" + quotaLimit);
      }
    }
    quotaMap = newMap;
  }

  @Override
  public int getPriorityLevel(Schedulable obj) {
    return 0;
  }

  @Override
  public boolean shouldBackOff(Schedulable obj) {
    if (obj.getUserGroupInformation() == null || obj.getMethodName() == null) {
      return false;
    }
    String userName = obj.getUserGroupInformation().getShortUserName();
    String methodName = obj.getMethodName();
    QuotaKey key = new QuotaKey(userName, methodName);
    Quota quota = quotaMap.get(key);
    if (quota != null) {
      return !quota.consume();
    } else {
      return false;
    }
  }

  public void addResponseTime(String name, int priorityLevel, int queueTime,
      int processingTime) {
  }

  @Override
  public void stop() {
  }
}
