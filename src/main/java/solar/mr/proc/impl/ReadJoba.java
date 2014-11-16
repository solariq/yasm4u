package solar.mr.proc.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.MRTableShard;

/**
* User: solar
* Date: 13.10.14
* Time: 8:04
*/
class ReadJoba implements MRJoba {
  private final String[] input;
  private final String output;
  private final Method method;

  public ReadJoba(String[] input, String output, Method method) {
    this.input = input;
    this.output = output;
    this.method = method;
  }

  @Override
  public boolean run(final MRWhiteboard wb) {
    final MRState state = wb.snapshot();
    final MRTableShard shard = state.get(input[0]);
    final ArrayBlockingQueue<CharSequence> seqs = new ArrayBlockingQueue<>(1000);
    final Thread readTh = new Thread(){
      @Override
      public void run() {
        try {
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
          seqs.put(CharSeqTools.EMPTY);
        }
        catch (RuntimeException re) {
          if (re.getCause() instanceof InterruptedException)
            return;
          throw re;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    readTh.start();
    try {
      final Constructor<?> constructor = method.getDeclaringClass().getConstructor(MRState.class);
      wb.set(output, method.invoke(constructor.newInstance(state), new Iterator<CharSequence>() {
        CharSequence next = null;
        boolean needs2wait = true;

        @Override
        public boolean hasNext() {
          if (next == null && needs2wait) {
            try {
              next = seqs.take();
              if (next == CharSeqTools.EMPTY) {
                needs2wait = false;
                next = null;
              }
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
          final CharSequence result = next;
          next = null;
          return result;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }));
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e) {
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
    return input;
  }

  @Override
  public String[] produces() {
    return new String[] {output};
  }
}
