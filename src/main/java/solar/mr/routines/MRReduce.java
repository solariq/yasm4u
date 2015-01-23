package solar.mr.routines;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.*;


import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.proc.State;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRReduce extends MRRoutine {
  public static final MRRecord EOF = new MRRecord("/dev/random", "", "", "");

  public static final int MAX_REDUCE_SIZE = 100000;
  private final Thread reduceThread;
  private final ArrayBlockingQueue<MRRecord> recordsQueue = new ArrayBlockingQueue<>(MAX_REDUCE_SIZE);

  public MRReduce(String[] inputTables, final MROutput output, State state) {
    super(inputTables, output, state);
    reduceThread = new Thread(new Runnable() {
      private MRRecord record;
      private MRRecord lastRetrieved;
      @Override
      public void run() {
        while (true) {
          try {
            if (record == null)
              record = recordsQueue.take();
            if (record == EOF)
              return;
            final String key = record.key;
            final Iterator<MRRecord> reduceIterator = new Iterator<MRRecord>() {
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
              public MRRecord next() {
                if (!hasNext())
                  throw new NoSuchElementException();
                final MRRecord current = record;
                record = null;
                return lastRetrieved = current;
              }

              @Override
              public void remove() {
                throw new RuntimeException("MR tables are read-only");
              }
            };
            try {
              final ExecutorService executor = Executors.newSingleThreadExecutor();
              try {
                final Future future = executor.submit(new Runnable() {
                  @Override
                  public void run() {
                    reduce(key, reduceIterator);
                  }
                });
                future.get(MAX_OPERATION_TIME, TimeUnit.SECONDS);
              } catch (TimeoutException e) {
                throw new RuntimeException("Reduce is too slow for key: " + key, e);
              } finally {
                executor.shutdownNow();
              }
            } catch (Exception e) {
              if (lastRetrieved != null) {
                output.error(e, lastRetrieved);
              } else {
                output.error(e, new MRRecord("No records were processed", "unknown", "unknown", "unknown"));
              }
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
  public final void invoke(MRRecord rec) {
    try {
      recordsQueue.put(rec);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected final void onEndOfInput() {
    super.onEndOfInput();
    try {
      try {
        recordsQueue.put(EOF);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      //reduceThread.interrupt();
      reduceThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract void reduce(String key, Iterator<MRRecord> reduce);
}
