package solar.mr;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRReduce extends MRRoutine {
  public static final int MAX_REDUCE_SIZE = 100000;
  private final Thread reduceThread;
  private final ArrayBlockingQueue<CharSeq> recordsQueue = new ArrayBlockingQueue<>(MAX_REDUCE_SIZE);

  public MRReduce(final MROutput output) {
    super(output);
    reduceThread = new Thread(new Runnable() {
      private CharSeq record;
      @Override
      public void run() {
        while (true) {
          try {
            if (record == null)
              record = recordsQueue.take();
            if (record == CharSeq.EMPTY)
              return;
            final CharSequence[] split = CharSeqTools.split(record, '\t');
            final String key = split[0].toString();
            final Iterator<Pair<String, CharSequence>> reduceIterator = new Iterator<Pair<String, CharSequence>>() {
              @Override
              public boolean hasNext() {
                while (record == null) {
                  try {
                    record = recordsQueue.take();
                  } catch (InterruptedException e) {
                    // skip, need to empty queue
                  }
                }
                if (record == CharSeq.EMPTY)
                  return false;
                //noinspection EqualsBetweenInconvertibleTypes
                return record.subSequence(0, key.length()).equals(key) && record.at(key.length()) == '\t';
              }

              @Override
              public Pair<String, CharSequence> next() {
                if (!hasNext())
                  throw new NoSuchElementException();
                final CharSeq keyChop = record.subSequence(key.length() + 1);
                int delim = keyChop.indexOf('\t');
                record = null;
                return new Pair<>(keyChop.sub(0, delim).toString(), (CharSequence) keyChop.subSequence(delim + 1));
              }

              @Override
              public void remove() {
                throw new RuntimeException("MR tables are read-only");
              }
            };
            try {
              reduce(key, reduceIterator);
            } catch (Exception e) {
              if (record != null)
                output.error(e, record);
            }
            while (reduceIterator.hasNext())
              reduceIterator.next();
          }
          catch (InterruptedException e) {
            //
          }
        }
      }
    }, "Reduce thread");
    reduceThread.setDaemon(true);
    reduceThread.start();
  }

  @Override
  public final void invoke(final CharSeq record) {
    recordsQueue.add(record);
    if (record == CharSeq.EMPTY) {
      try {
        reduceThread.interrupt();
        reduceThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public abstract void reduce(String key, Iterator<Pair<String, CharSequence>> reduce);
}
