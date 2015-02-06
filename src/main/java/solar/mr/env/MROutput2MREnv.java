package solar.mr.env;

import com.spbsu.commons.io.QueueReader;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqChar;
import com.spbsu.commons.seq.CharSeqComposite;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

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
    queues = new ArrayBlockingQueue[output.length];
    for(int i = 0; i < output.length; i++) {
      final ArrayBlockingQueue<CharSeq> queue = new ArrayBlockingQueue<>(1000);
      final MRPath path = output[i];
      queues[i] = queue;
      final Thread thread = new Thread("MR parallel output thread to " + path.resource()) {
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
    queues[tableNo].add(new CharSeqComposite(record, new CharSeqChar('\n')));
  }

  public void interrupt() {
    for(int i = 0; i < queues.length; i++) {
      queues[i].add(CharSeq.EMPTY);
    }
  }
}