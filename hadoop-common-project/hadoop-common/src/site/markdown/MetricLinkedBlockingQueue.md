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

The MetricLinkedBlockingQueue-related configurations are relevant to only a single IPC server. That's the same as the
Fair Call Queue. Each configuration is prefixed with `ipc.<port_number>`, where `<port_number>` is the port used by
the IPC server to be configured.

### Full List of Configurations

| Configuration Key | Applicable Component | Description | Default |
|:---- |:---- |:---- |:--- |
| callqueue.impl | General | The fully qualified name of a class to use as the implementation of a call queue. Use `org.apache.hadoop.ipc.MetricLinkedBlockingQueue` for the MetricLinkedBlockingQueue. | `java.util.concurrent.LinkedBlockingQueue` (FIFO queue) |
| readerqueue.impl | General | The fully qualified name of a class to use as the implementation of a reader queue. Use `org.apache.hadoop.ipc.MetricLinkedBlockingQueue` for the MetricLinkedBlockingQueue. | `java.util.concurrent.LinkedBlockingQueue` (FIFO queue) |
| metric.blocking.queue.interval | MetricLinkedBlockingQueue | The time interval to compute ops(operations per seconds). Time unit is milliseconds. | `300000` (5 minutes) |
| metric.blocking.queue.log.threshold | MetricLinkedBlockingQueue | When the queue's size reaches queue's max size * threshold, the queue will write a log. | `1.0f` (write log when the queue is full) |

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
    <property>
         <name>ipc.56200.metric.blocking.queue.interval</name>
         <value>300000</value>
    </property>
    <property>
         <name>ipc.56200.metric.blocking.queue.log.threshold</name>
         <value>1.0f</value>
    </property>

### Statistics in jmx

We can find the statistics from the ipc server's jmx. Using 'qry' helps us locating the jmx quickly. Supposing we have 2 reader queues and use the configuration in the above example, the url will be:

    // The statistics of the first reader queue.
    http://<ipc host>:<http_port>/jmx?qry=Hadoop:service=ipc.56200.reader-1,name=MetricLinkedBlockingQueue
    // The statistics of the second reader queue.
    http://<ipc host>:<http_port>/jmx?qry=Hadoop:service=ipc.56200.reader-2,name=MetricLinkedBlockingQueue
    // The statistics of the call queue.
    http://c4-hadoop-tst-ct02.bj:56201/jmx?qry=Hadoop:service=ipc.56200,name=MetricLinkedBlockingQueue

### Explanation of statistical items

MetricLinkedBlockingQueue statistics 4 kinds operations: put to the queue(Put), take from the queue(Take), poll from the queue(Poll),
poll from the queue with the returned value is not null(PollNotNull).

| Item | Description |
|:---- |:---- |
| LastPutPS | Put per second in the last interval. |
| LastTakePS | Take per second in the last interval. |
| LastPollPS |  Poll per second in the last interval.|
| LastPollNotNullPS | Poll(with returned value not null) per second in the last interval. |
| CurrentTotalPut | The total Put operations in the current interval. |
| CurrentTotalTake | The total Take operations in the current interval. |
| CurrentTotalPoll | The total Poll operations in the current interval. |
| CurrentTotalPollNotNull | The total Poll(with returned value not null) operations in the current interval. |
| CurrentStartTime | The start timestamp of the current interval. |
| ComputeInterval | The interval in millie seconds. |