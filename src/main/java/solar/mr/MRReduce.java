package solar.mr;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.util.Pair;
import solar.mr.proc.MRState;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRReduce extends MRRoutine {
  public static final Record EOF = new Record("/dev/random", "", "", "");

  public static final int MAX_REDUCE_SIZE = 100000;
  private final Thread reduceThread;
  private final ArrayBlockingQueue<Record> recordsQueue = new ArrayBlockingQueue<>(MAX_REDUCE_SIZE);

  public MRReduce(String[] inputTables, final MROutput output, MRState state) {
    super(inputTables, output, state);
    reduceThread = new Thread(new Runnable() {
      private Record record;
      private Record lastRetrieved;
      @Override
      public void run() {
        while (true) {
          try {
            if (record == null)
              record = recordsQueue.take();
            if (record == EOF)
              return;
            final String key = record.key;
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
                if (record == EOF)
                  return false;
                //noinspection EqualsBetweenInconvertibleTypes
                return key.equals(record.key);
              }

              @Override
              public Pair<String, CharSequence> next() {
                if (!hasNext())
                  throw new NoSuchElementException();
                final Record current = record;
                record = null;
                lastRetrieved = current;
                return new Pair<>(current.sub, current.value);
              }

              @Override
              public void remove() {
                throw new RuntimeException("MR tables are read-only");
              }
            };
            try {
              reduce(key, reduceIterator);
            } catch (Exception e) {
              if (lastRetrieved != null)
                output.error(e, currentTable(), lastRetrieved.toString());
              interrupt();
              break;
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
  public final void invoke(Record rec) {
    recordsQueue.add(rec);
  }

  @Override
  protected final void onEndOfInput() {
    super.onEndOfInput();
    try {
      recordsQueue.add(EOF);
      reduceThread.interrupt();
      reduceThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract void reduce(String key, Iterator<Pair<String, CharSequence>> reduce);
}
