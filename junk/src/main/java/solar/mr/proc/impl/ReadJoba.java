package solar.mr.proc.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.func.Processor;
import solar.mr.MRTable;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.proc.tags.MRRead;
import solar.mr.tables.MRTableShard;

/**
* User: solar
* Date: 13.10.14
* Time: 8:04
*/
class ReadJoba implements MRJoba {
  private final MRRead readAnn;
  private final Method method;

  public ReadJoba(final MRRead readAnn, final Method method) {
    this.readAnn = readAnn;
    this.method = method;
  }

  @Override
  public boolean run(final MRWhiteboard wb) {
    final MRState state = wb.slice();
    final MRTable input = state.get(readAnn.input());
    final ArrayBlockingQueue<CharSequence> seqs = new ArrayBlockingQueue<>(1000);
    final Thread readTh = new Thread(){
      @Override
      public void run() {
        try {
          for (MRTableShard shard : wb.env().shards(input)) {
            wb.env().read(shard, new Processor<CharSequence>() {
              @Override
              public void process(final CharSequence arg) {
                try {
                  seqs.put(arg);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
          }
        }
        catch (RuntimeException re) {
          if (re.getCause() instanceof InterruptedException)
            return;
          throw re;
        }
      }
    };
    readTh.start();
    try {
      wb.set(readAnn.output(), method.invoke(new Iterator<CharSequence>() {
        CharSequence next = null;
        boolean needs2wait = true;

        @Override
        public boolean hasNext() {
          if (next == null && needs2wait) {
            try {
              next = seqs.take();
            } catch (InterruptedException e) {
              needs2wait = false;
            }
          }
          return next != null;
        }

        @Override
        public CharSequence next() {
          if (!hasNext())
            throw new NoSuchElementException();
          return next;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }));
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    try {
      readTh.join();
    }
    catch (InterruptedException ie) {
      // skip
    }

    return true;
  }

  @Override
  public String[] consumes() {
    return new String[] {readAnn.input()};
  }

  @Override
  public String[] produces() {
    return new String[] {readAnn.output()};
  }
}
