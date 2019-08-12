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

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.ipc.MetricLinkedBlockingQueue.OP;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_METRIC_BLOCKING_QUEUE_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Basic tests of MetricLinkedBlockingQueue.
 */
public class TestMetricLinkedBlockingQueue {

  private MetricLinkedBlockingQueue mlbq;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    String prefix = "ipc.0";
    conf.setLong(prefix + "." + IPC_METRIC_BLOCKING_QUEUE_INTERVAL, 3 * 1000);
    mlbq = new MetricLinkedBlockingQueue<>(5, prefix, "ns", conf);
  }

  @Test(timeout = 10000)
  public void testPollReturnsNullWhenEmpty() {
    assertNull(mlbq.poll());
  }

  @Test(timeout = 10000)
  public void testQueueTotalComputation() throws InterruptedException {
    assertEquals(0, mlbq.getQueueTotal());
    for (int i = 0; i < 5; i++) {
      mlbq.put(new Object());
      assertEquals(i + 1, mlbq.getQueueTotal());
    }
    for (int i = 0; i < 5; i++) {
      mlbq.take();
      assertEquals(4 - i, mlbq.getQueueTotal());
    }
  }

  @Test(timeout = 10000)
  public void testOps() throws Exception {
    for (int i = 0; i < 6; i++) {
      mlbq.put(new Object());
      mlbq.put(new Object());
      mlbq.take();
      mlbq.poll(10, TimeUnit.MILLISECONDS);
    }
    for (int i = 0; i < 3; i++) {
      mlbq.poll(10, TimeUnit.MILLISECONDS);
    }

    // sleep and put to trigger qps update
    Thread.sleep(3 * 1000);
    mlbq.put(new Object());

    assertEquals(6 * 2 / 3, mlbq.getOps()[OP.PUT.ordinal()]);
    assertEquals(6 * 1 / 3, mlbq.getOps()[OP.TAKE.ordinal()]);
    assertEquals(6 * 1 / 3, mlbq.getOps()[OP.POLL_NOT_NULL.ordinal()]);
    assertEquals((6 + 3) / 3, mlbq.getOps()[OP.POLL.ordinal()]);
  }
}
