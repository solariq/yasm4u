package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.io.QueueReader;
import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqChar;
import com.expleague.commons.seq.CharSeqComposite;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User: solar
 * Date: 29.01.15
 * Time: 19:44
 */
public class MROutput2MREnv extends MROutputBase {
  private final ArrayBlockingQueue<CharSeq>[] queues;
  private final List<Thread> outputThreads = new ArrayList<>();
  private final MRErrorsHandler handler;

  public MROutput2MREnv(final MREnv env, final MRPath[] output, MRErrorsHandler handler) {
    super(output);
    this.handler = handler;
    //noinspection unchecked
    queues = new ArrayBlockingQueue[output.length + 1];
    for(int i = 0; i < output.length + 1; i++) {
      final ArrayBlockingQueue<CharSeq> queue = new ArrayBlockingQueue<>(1000);
      final MRPath path = i == output.length ? MRPath.create("/dev/null") : output[i];
      queues[i] = queue;
      final Thread thread = new Thread("MR parallel output thread to " + path.toURI()) {
        @Override
        public void run() {
          env.write(path, new QueueReader(queue));
        }
      };
      thread.setDaemon(true);
      thread.start();
      outputThreads.add(thread);
    }
  }

  public void join() {
    try {
      for (Thread thread : outputThreads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void error(Throwable th, MRRecord rec) {
    if (handler != null)
      handler.error(th, rec);
    else
      super.error(th, rec);
  }

  @Override
  public void error(final String type, final String cause, final MRRecord rec) {
    if (handler != null)
      handler.error(type, cause, rec);
    else
      super.error(type, cause, rec);
  }

  @Override
  public int errorsCount() {
    return handler != null ? handler.errorsCount() : super.errorsCount();
  }

  @Override
  protected void push(int tableNo, CharSequence record) {
    try {
      queues[tableNo].put(new CharSeqComposite(record, new CharSeqChar('\n')));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    interrupt();
  }

  public void interrupt() {
    for(int i = 0; i < queues.length; i++) {
      try {
        queues[i].put(CharSeq.EMPTY);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
