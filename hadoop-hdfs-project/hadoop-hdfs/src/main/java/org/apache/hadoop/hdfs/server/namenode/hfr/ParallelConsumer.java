package org.apache.hadoop.hdfs.server.namenode.hfr;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ParallelConsumer helps consuming a queue in parallel. The initial elements
 * should be added to the queue before execute(), and new elements can be added
 * to the queue while consuming elements. An element is consumed only when
 * consume(T element) returns.
 * Method execute() blocks until all elements are consumed. If any Exception is
 * thrown by the consumer, the execution will be aborted and an IOException will
 * be thrown by execute().
 * Method execute() could also be interrupted. It will
 * abort the job and
 */
public abstract class ParallelConsumer<T,K> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ParallelConsumer.class);
  private int threadNum;
  private int finished;
  private LinkedList<T> queue;
  private int onGoing;

  class Consumer implements Callable<Object> {
    K attach;

    public Consumer(K attach) {
      this.attach = attach;
    }

    @Override
    public Object call() throws Exception {
      T element;
      while ((element = next()) != null) {
        try {
          consume(element, attach);
        } catch (Throwable t) {
          throw t;
        } finally {
          finish();
        }
      }
      return null;
    }
  }

  ParallelConsumer(int threadNum) {
    this.threadNum = threadNum;
    this.queue = new LinkedList<>();
    this.onGoing = 0;
  }

  /**
   * Consume an element.
   */
  public abstract void consume(T element, K attach) throws Exception;

  /**
   * Add elements to the queue.
   */
  protected synchronized void add(T[] elements) {
    for (T element : elements) {
      queue.addLast(element);
    }
    notifyAll();
  }

  /**
   * Add elements to the queue.
   */
  protected synchronized void add(T element) {
    queue.addLast(element);
    notifyAll();
  }

  /**
   * This method waits until all elements are consumed or all threads finish.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException if any exception happens in sub-threads or the queue is
   *           not empty after all the sub-threads finish.
   */
  public void execute() throws InterruptedException, IOException {
    finished = 0;
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, threadNum,
        1, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(threadNum));
    CompletionService<Object> service =
        new ExecutorCompletionService<>(executor);
    try {
      for (int i = 0; i < threadNum; i++) {
        K attach = threadAttach();
        Consumer callable = new Consumer(attach);
        service.submit(callable);
      }
      for (int i = 0; i < threadNum; i++) {
        try {
          service.take().get();
        } catch (ExecutionException e) {
          throw new IOException(
              "Sub thread fails during execution, abort the job.", e);
        } finally {
          finished++;
        }
      }
    } finally {
      executor.shutdownNow();
    }
    if (queue.size() > 0) {
      throw new IOException("The queue is not empty !");
    }
  }

  @VisibleForTesting
  int getFinished() {
    return finished;
  }

  /**
   * Add an attachment to the working thread.
   */
  protected K threadAttach() {
    return null;
  }

  private synchronized T next() {
    while (queue.isEmpty()) {
      if (onGoing > 0) {
        try {
          wait();
        } catch (InterruptedException e) {
          return null;
        }
      } else {
        return null;
      }
    }
    onGoing++;
    return queue.removeFirst();
  }

  private synchronized void finish() {
    onGoing--;
    if (onGoing == 0) {
      notifyAll();
    }
  }

  private synchronized void error() {
    notifyAll();
  }
}
