<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

MetricLinkedBlockingQueue Guide
=================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This document describes how to configure and manage the MetricLinkedBlockingQueue for Hadoop.

Prerequisites
-------------

Make sure Hadoop is installed, configured and setup correctly. For more information see:

* [Single Node Setup](./SingleCluster.html) for first-time users.
* [Cluster Setup](./ClusterSetup.html) for large, distributed clusters.

Overview
--------

Hadoop enables admin to configure the readers' and handlers' size. If we know which is the bottleneck,
we can set a more reasonable value to the number. MetricLinkedBlockingQueue is used for profiling the
hadoop ipc server to find the bottleneck of it. It statistics the put, poll and take operations of the queue,
log when the queue is full. Based on the statistical information we can adjust the size of reader and handler.

Configuration
-------------

This section describes how to configure the MetricLinkedBlockingQueue.

### Configuration Prefixes

因为Reader有很多啊，所以就没办法搞成全部是ipc.port.xxx这种配置形式。
ipc.metric.blocking.queue.interval就是只有这一项，所有用了MetricLinkedBlockingQueue的都一样；
还有IPC_METRIC_BLOCKING_QUEUE_LOG_THRESHOLD=ipc.metric.blocking.queue.log.threshold，也是没办法。

The MetricLinkedBlockingQueue-related configurations are relevant to only a single IPC server. That's the same as the
Fair Call Queue. Each configuration is prefixed with `ipc.<port_number>`, where `<port_number>` is the port used by
the IPC server to be configured.

### Full List of Configurations

| Configuration Key | Applicable Component | Description | Default |
|:---- |:---- |:---- |:--- |
| callqueue.impl | General | The fully qualified name of a class to use as the implementation of a call queue. Use `org.apache.hadoop.ipc.MetricLinkedBlockingQueue` for the MetricLinkedBlockingQueue. | `java.util.concurrent.LinkedBlockingQueue` (FIFO queue) |
| readerqueue.impl | General | The fully qualified name of a class to use as the implementation of a reader queue. Use `org.apache.hadoop.ipc.MetricLinkedBlockingQueue` for the MetricLinkedBlockingQueue. | `java.util.concurrent.LinkedBlockingQueue` (FIFO queue) |

### Example Configuration

This is an example of configuration an IPC server at port 8020 to use `MetricLinkedBlockingQueue` for both call queue and reader queue.

    <property>
         <name>ipc.56200.readerqueue.impl</name>
         <value>org.apache.hadoop.ipc.MetricLinkedBlockingQueue</value>
    </property>
    <property>
         <name>ipc.56200.callqueue.impl</name>
         <value>org.apache.hadoop.ipc.MetricLinkedBlockingQueue</value>
    </property>

这里再写一下url吧，怎么在jmx上看结果！怎么看每个队列的结果！