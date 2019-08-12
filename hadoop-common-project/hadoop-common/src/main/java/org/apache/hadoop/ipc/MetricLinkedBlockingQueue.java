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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_METRIC_BLOCKING_QUEUE_INTERVAL;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_METRIC_BLOCKING_QUEUE_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_METRIC_BLOCKING_QUEUE_LOG_THRESHOLD;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_METRIC_BLOCKING_QUEUE_LOG_THRESHOLD_DEFAULT;

/**
 * Compute ops and write log when the queue is full. It's used to monitor the
 * rpc queues(Reader queue and Call queue).
 */
public class MetricLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
  final private long interval;
  enum OP {
    TAKE, PUT, POLL, POLL_NOT_NULL
  }
  private AtomicInteger[] total;
  private final int[] ops = new int[OP.values().length];
  private volatile long startTime;

  private int logSize;
  private AtomicInteger queueTotal;

  public MetricLinkedBlockingQueue(int size, String prefix, String ns,
      Configuration conf) {
    super(size);
    this.interval =
        conf.getLong(prefix + "." + IPC_METRIC_BLOCKING_QUEUE_INTERVAL,
            IPC_METRIC_BLOCKING_QUEUE_INTERVAL_DEFAULT);
    total = new AtomicInteger[OP.values().length];
    for (OP o : OP.values()) {
      total[o.ordinal()] = new AtomicInteger();
    }
    startTime = Time.monotonicNow();
    queueTotal = new AtomicInteger();
    logSize = (int) (
        conf.getFloat(prefix + "." + IPC_METRIC_BLOCKING_QUEUE_LOG_THRESHOLD,
            IPC_METRIC_BLOCKING_QUEUE_LOG_THRESHOLD_DEFAULT) * size);
    // Make this the active source of metrics
    MetricsProxy mp = MetricsProxy.getInstance(ns);
    mp.setDelegate(this);
  }

  @Override
  public void put(E e) throws InterruptedException {
    super.put(e);
    updateQps(OP.PUT);
    if (logSize > 0) {
      int qt = queueTotal.incrementAndGet();
      if (qt >= logSize) {
        Server.LOG.info("[PUT] the queue size is " + qt);
      }
    }
  }

  @Override
  public E take() throws InterruptedException {
    E e = super.take();
    updateQps(OP.TAKE);
    if (logSize > 0) {
      int qt = queueTotal.decrementAndGet();
      if (qt >= logSize - 1) {
        Server.LOG.info("[TAKE] the queue size is " + qt);
      }
    }
    return e;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    E e = super.poll(timeout, unit);
    updateQps(OP.POLL);
    if (e != null) {
      updateQps(OP.POLL_NOT_NULL);
    }
    if (logSize > 0 && e != null) {
      int qt = queueTotal.decrementAndGet();
      if (qt >= logSize - 1) {
        Server.LOG.info("[POLL] the queue size is " + qt);
      }
    }
    return e;
  }

  @Override
  public E poll() {
    E e = super.poll();
    updateQps(OP.POLL);
    if (e != null) {
      updateQps(OP.POLL_NOT_NULL);
    }
    if (logSize > 0 && e != null) {
      int qt = queueTotal.decrementAndGet();
      if (qt >= logSize - 1) {
        Server.LOG.info("[POLL] the queue size is " + qt);
      }
    }
    return e;
  }

  private void updateQps(OP op) {
    long current = Time.monotonicNow();
    if (current - startTime < interval) {
      total[op.ordinal()].incrementAndGet();
    } else {
      synchronized (ops) {
        if (current - startTime >= interval) {
          startTime = current;
          for (OP o : OP.values()) {
            ops[o.ordinal()] = updateQps(total[o.ordinal()]);
          }
        }
      }
      total[op.ordinal()].incrementAndGet();
    }
  }

  private int updateQps(AtomicInteger ai) {
    int aiTotal = ai.get();
    ai.set(0);
    return aiTotal / (int)(interval / 1000);
  }

  public int[] getOps() {
    int[] res = new int[OP.values().length];
    synchronized (ops) {
      for (OP o : OP.values()) {
        res[o.ordinal()] = ops[o.ordinal()];
      }
    }
    return res;
  }

  @VisibleForTesting
  int getQueueTotal() {
    return this.queueTotal.get();
  }

  /**
   * MetricsProxy is a singleton because we may init multiple
   * MetricLinkedBlockingQueues, but the metrics system cannot unregister beans
   * cleanly.
   */
  private static final class MetricsProxy
      implements MetricLinkedBlockingQueueMXBean {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
        new HashMap<>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<MetricLinkedBlockingQueue> delegate;
    private int revisionNumber = 0;

    private MetricsProxy(String namespace) {
      MBeans.register(namespace, "MetricLinkedBlockingQueue", this);
    }

    public static synchronized MetricsProxy getInstance(String namespace) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public void setDelegate(MetricLinkedBlockingQueue obj) {
      this.delegate = new WeakReference<>(obj);
      this.revisionNumber++;
    }

    @Override
    public int getLastPutPS() {
      return getLastQps(OP.PUT);
    }

    @Override
    public int getLastTakePS() {
      return getLastQps(OP.TAKE);
    }

    @Override
    public int getLastPollPS() {
      return getLastQps(OP.POLL);
    }

    @Override
    public int getLastPollNotNullPS() {
      return getLastQps(OP.POLL_NOT_NULL);
    }

    private int getLastQps(OP op) {
      MetricLinkedBlockingQueue obj = this.delegate.get();
      if (obj == null) {
        return -1;
      }
      return obj.getOps()[op.ordinal()];
    }

    @Override
    public int getCurrentTotalPut() {
      return getCurrentTotal(OP.PUT);
    }

    @Override
    public int getCurrentTotalTake() {
      return getCurrentTotal(OP.TAKE);
    }

    @Override
    public int getCurrentTotalPoll() {
      return getCurrentTotal(OP.POLL);
    }

    @Override
    public int getCurrentTotalPollNotNull() {
      return getCurrentTotal(OP.POLL_NOT_NULL);
    }

    private int getCurrentTotal(OP op) {
      MetricLinkedBlockingQueue obj = this.delegate.get();
      if (obj == null) {
        return -1;
      }
      return obj.total[op.ordinal()].get();
    }

    @Override
    public long getCurrentStartTime() {
      MetricLinkedBlockingQueue obj = this.delegate.get();
      if (obj == null) {
        return -1;
      }
      return obj.startTime;
    }

    @Override
    public long getComputeInterval() {
      MetricLinkedBlockingQueue obj = this.delegate.get();
      if (obj == null) {
        return -1;
      }
      return obj.interval;
    }

    @Override
    public int getRevision() {
      return this.revisionNumber;
    }
  }
}
