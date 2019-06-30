package org.apache.hadoop.ipc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts queue operations for different blocking queues.
 */
public class SwapQueueManager<E> extends AbstractQueue<E>
    implements BlockingQueue<E> {
  public static final Logger LOG =
      LoggerFactory.getLogger(SwapQueueManager.class);
  // Number of checkpoints for empty queue.
  private static final int CHECKPOINT_NUM = 20;
  // Interval to check empty queue.
  private static final long CHECKPOINT_INTERVAL_MS = 10;

  @SuppressWarnings("unchecked")
  static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
      Class<?> queueClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queueClass;
  }

  // Atomic refs point to active callQueue
  // We have two so we can better control swapping
  final AtomicReference<BlockingQueue<E>> putRef;
  final AtomicReference<BlockingQueue<E>> takeRef;

  public SwapQueueManager(Class<? extends BlockingQueue<E>> backingClass,
      int maxQueueSize, String namespace, Configuration conf) {
    BlockingQueue<E> bq = createQueueInstance(backingClass,
        maxQueueSize, namespace, conf);
    this.putRef = new AtomicReference<>(bq);
    this.takeRef = new AtomicReference<>(bq);
    LOG.info("Using swapQueue: {}, queueCapacity: {}.", backingClass,
        maxQueueSize);
  }

  SwapQueueManager(BlockingQueue<E> queue) {
    this.putRef = new AtomicReference<>(queue);
    this.takeRef = new AtomicReference<>(queue);
    LOG.info("Using swapQueue: {}, queueCapacity: {}.", queue.getClass(),
        queue.size());
  }

  static <T extends BlockingQueue> T createQueueInstance(Class<T> theClass,
      int maxLen, String ns, Configuration conf) {
    // Used for custom, configurable callqueues
    try {
      Constructor<T> ctor = theClass
          .getDeclaredConstructor(int.class, String.class, Configuration.class);
      return ctor.newInstance(maxLen, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(maxLen);
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

  @Override
  public void put(E e) throws InterruptedException {
    putRef.get().put(e);
  }

  @Override
  public boolean add(E e) {
    return putRef.get().add(e);
  }

  /**
   * Insert e into the queue.
   * Return true if e is queued.
   * Return false if the queue is full.
   */
  @Override
  public boolean offer(E e) {
    return putRef.get().offer(e);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    return putRef.get().offer(e, timeout, unit);
  }

  @Override
  public E peek() {
    return takeRef.get().peek();
  }

  @Override
  public E poll() {
    return takeRef.get().poll();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return takeRef.get().poll(timeout, unit);
  }

  /**
   * Retrieve an E from the queue or block until we can.
   * Guaranteed to return an element from the current queue.
   */
  @Override
  public E take() throws InterruptedException {
    E e = null;
    while (e == null) {
      e = takeRef.get().poll(1000L, TimeUnit.MILLISECONDS);
    }
    return e;
  }

  @Override
  public int size() {
    return takeRef.get().size();
  }

  @Override
  public int remainingCapacity() {
    return takeRef.get().remainingCapacity();
  }

  /**
   * Replaces active queue with the newly requested one and transfers
   * all calls to the newQ before returning.
   */
  public synchronized void swapQueue(
      Class<? extends RpcScheduler> schedulerClass,
      Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize,
      String ns, Configuration conf) {
    BlockingQueue<E> newQ =
        createQueueInstance(queueClassToUse, maxSize, ns, conf);

    // Our current queue becomes the old queue
    BlockingQueue<E> oldQ = putRef.get();

    // Swap putRef first: allow blocked puts() to be unblocked
    putRef.set(newQ);

    // Wait for handlers to drain the oldQ
    while (!queueIsReallyEmpty(oldQ)) {}

    // Swap takeRef to handle new calls
    takeRef.set(newQ);

    LOG.info("Old Queue: " + stringRepr(oldQ) + ", " +
        "Replacement: " + stringRepr(newQ));
  }

  /**
   * Checks if queue is empty by checking at CHECKPOINT_NUM points with
   * CHECKPOINT_INTERVAL_MS interval.
   * This doesn't mean the queue might not fill up at some point later, but
   * it should decrease the probability that we lose a call this way.
   */
  private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
    for (int i = 0; i < CHECKPOINT_NUM; i++) {
      try {
        Thread.sleep(CHECKPOINT_INTERVAL_MS);
      } catch (InterruptedException ie) {
        return false;
      }
      if (!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private String stringRepr(Object o) {
    return o.getClass().getName() + '@' + Integer.toHexString(o.hashCode());
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return takeRef.get().drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    return takeRef.get().drainTo(c, maxElements);
  }

  @Override
  public Iterator<E> iterator() {
    return takeRef.get().iterator();
  }
}
