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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts queue operations for different blocking queues.
 */
public class CallQueueManager<E extends Schedulable>
    extends SwappableQueueManager<E> implements BlockingQueue<E> {
  public static final Logger LOG =
      LoggerFactory.getLogger(CallQueueManager.class);

  @SuppressWarnings("unchecked")
  static Class<? extends RpcScheduler> convertSchedulerClass(
      Class<?> schedulerClass) {
    return (Class<? extends RpcScheduler>)schedulerClass;
  }

  private volatile boolean clientBackOffEnabled;

  private RpcScheduler scheduler;

  public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
                          Class<? extends RpcScheduler> schedulerClass,
      boolean clientBackOffEnabled, int maxQueueSize, String namespace,
      Configuration conf) {
    super(createQueueInstance(backingClass, parseNumLevels(namespace, conf),
        maxQueueSize, namespace, conf));
    int priorityLevels = parseNumLevels(namespace, conf);
    this.scheduler = createScheduler(schedulerClass, priorityLevels,
        namespace, conf);
    this.clientBackOffEnabled = clientBackOffEnabled;
    LOG.info("Using callQueue: {}, queueCapacity: {}, " +
        "scheduler: {}, ipcBackoff: {}.",
        backingClass, maxQueueSize, schedulerClass, clientBackOffEnabled);
  }

  @VisibleForTesting // only!
  CallQueueManager(BlockingQueue<E> queue, RpcScheduler scheduler,
      boolean clientBackOffEnabled) {
    super(queue);
    this.scheduler = scheduler;
    this.clientBackOffEnabled = clientBackOffEnabled;
  }

  private static <T extends RpcScheduler> T createScheduler(
      Class<T> theClass, int priorityLevels, String ns, Configuration conf) {
    // Used for custom, configurable scheduler
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          String.class, Configuration.class);
      return ctor.newInstance(priorityLevels, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(priorityLevels);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
        " could not be constructed.");
  }

  static <T extends BlockingQueue> T createQueueInstance(
      Class<T> theClass, int priorityLevels, int maxLen, String ns,
      Configuration conf) {

    // Used for custom, configurable callqueues
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          int.class, String.class, Configuration.class);
      return ctor.newInstance(priorityLevels, maxLen, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    return createQueueInstance(theClass, maxLen, ns, ns, conf);
  }

  boolean isClientBackoffEnabled() {
    return clientBackOffEnabled;
  }

  // Based on policy to determine back off current call
  boolean shouldBackOff(Schedulable e) {
    return scheduler.shouldBackOff(e);
  }

  void addResponseTime(String name, Schedulable e, ProcessingDetails details) {
    scheduler.addResponseTime(name, e, details);
  }

  // This should be only called once per call and cached in the call object
  // each getPriorityLevel call will increment the counter for the caller
  int getPriorityLevel(Schedulable e) {
    return scheduler.getPriorityLevel(e);
  }

  void setClientBackoffEnabled(boolean value) {
    clientBackOffEnabled = value;
  }

  /**
   * Insert e into the backing queue or block until we can.  If client
   * backoff is enabled this method behaves like add which throws if
   * the queue overflows.
   * If we block and the queue changes on us, we will insert while the
   * queue is drained.
   */
  @Override
  public void put(E e) throws InterruptedException {
    if (!isClientBackoffEnabled()) {
      super.put(e);
    } else if (shouldBackOff(e)) {
      throwBackoff();
    } else {
      // No need to re-check backoff criteria since they were just checked
      addInternal(e, false);
    }
  }

  @Override
  public boolean add(E e) {
    return addInternal(e, true);
  }

  @VisibleForTesting
  boolean addInternal(E e, boolean checkBackoff) {
    if (checkBackoff && isClientBackoffEnabled() && shouldBackOff(e)) {
      throwBackoff();
    }
    try {
      return super.add(e);
    } catch (CallQueueOverflowException ex) {
      // queue provided a custom exception that may control if the client
      // should be disconnected.
      throw ex;
    } catch (IllegalStateException ise) {
      throwBackoff();
    }
    return true;
  }

  // ideally this behavior should be controllable too.
  private void throwBackoff() throws IllegalStateException {
    throw CallQueueOverflowException.DISCONNECT;
  }

  /**
   * Read the number of levels from the configuration.
   * This will affect the FairCallQueue's overall capacity.
   * @throws IllegalArgumentException on invalid queue count
   */
  @SuppressWarnings("deprecation")
  private static int parseNumLevels(String ns, Configuration conf) {
    // Fair call queue levels (IPC_CALLQUEUE_PRIORITY_LEVELS_KEY)
    // takes priority over the scheduler level key
    // (IPC_SCHEDULER_PRIORITY_LEVELS_KEY)
    int retval = conf.getInt(ns + "." +
        FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 0);
    if (retval == 0) { // No FCQ priority level configured
      retval = conf.getInt(ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY,
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_DEFAULT_KEY);
    } else {
      LOG.warn(ns + "." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY +
          " is deprecated. Please use " + ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY + ".");
    }
    if(retval < 1) {
      throw new IllegalArgumentException("numLevels must be at least 1");
    }
    return retval;
  }

  /**
   * Replaces active queue and scheduler with the newly requested one.
   */
  public synchronized void swapQueueAndRpcScheduler(
      Class<? extends RpcScheduler> schedulerClass,
      Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize, String ns,
      Configuration conf) {
    int priorityLevels = parseNumLevels(ns, conf);
    this.scheduler.stop();
    RpcScheduler newScheduler =
        createScheduler(schedulerClass, priorityLevels, ns, conf);
    BlockingQueue<E> newQ = createQueueInstance(queueClassToUse,
        priorityLevels, maxSize, ns, conf);
    super.swapQueue(newQ);
    this.scheduler = newScheduler;
  }

  // exception that mimics the standard ISE thrown by blocking queues but
  // embeds a rpc server exception for the client to retry and indicate
  // if the client should be disconnected.
  @SuppressWarnings("serial")
  static class CallQueueOverflowException extends IllegalStateException {
    private static String TOO_BUSY = "Server too busy";
    static final CallQueueOverflowException KEEPALIVE =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY),
            RpcStatusProto.ERROR);
    static final CallQueueOverflowException DISCONNECT =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY + " - disconnecting"),
            RpcStatusProto.FATAL);

    CallQueueOverflowException(final IOException ioe,
        final RpcStatusProto status) {
      super("Queue full", new RpcServerException(ioe.getMessage(), ioe){
        @Override
        public RpcStatusProto getRpcStatusProto() {
          return status;
        }
      });
    }
    @Override
    public IOException getCause() {
      return (IOException)super.getCause();
    }
  }
}
