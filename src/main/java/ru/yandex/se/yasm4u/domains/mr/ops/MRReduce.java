package ru.yandex.se.yasm4u.domains.mr.ops;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.*;


import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.env.MROutputBase;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRReduce extends MROperation {
  public static final MRRecord EOF = new MRRecord(MRPath.create("/dev/random"), "", "", "");

  public static final int MAX_REDUCE_SIZE = 10000;
  private final Thread reduceThread;
  private final ArrayBlockingQueue<MRRecord> recordsQueue = new ArrayBlockingQueue<>(MAX_REDUCE_SIZE);
  protected MROutput output;

  public MRReduce(MRPath[] inputTables, final MROutput output, State state) {
    super(inputTables, output, state);
    this.output= output;
    reduceThread = new Thread(new Runnable() {
      private MRRecord record;
      private MRRecord lastRetrieved;
      @Override
      public void run() {
        while (true) {
          try {
            if (record == null)
              record = recordsQueue.poll(MAX_OPERATION_TIME, TimeUnit.SECONDS);
              if (record == null) {
                throw new RuntimeException("key: record is null");
              }
            if (record == EOF) {
              dumpThread("EOF received");
              break;
            }
            final String key = record.key;
            final Iterator<MRRecord> reduceIterator = new Iterator<MRRecord>() {
              @Override
              public boolean hasNext() {
                while (record == null) {
                  try {
                    record = recordsQueue.poll(MAX_OPERATION_TIME, TimeUnit.SECONDS);
                    if (record == null) {
                      throw new RuntimeException("key: record is null");
                    }
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
              reduce(key, reduceIterator);
            } catch (Exception e) {
              if (lastRetrieved != null) {
                output.error(e, lastRetrieved);
              } else {
                output.error(e, new MRRecord(MRPath.create("/dev/random"), "unknown", "unknown", "unknown"));
              }
              interrupt();
              StringWriter writer = new StringWriter();
              e.printStackTrace(new PrintWriter(writer));
              dumpThread("Exception:" + writer.toString());
              break;
            }
            while (reduceIterator.hasNext())
              reduceIterator.next();
          }
          catch (InterruptedException e) {
            dumpThread("InterruptedException: " + e.getMessage());
            //
          }
        } /** end of while **/
        MROperation.dumpThread("loop exit");
        ((MROutputBase)output).stop();
      }
    }, "Reduce thread");
    reduceThread.setDaemon(true);
    reduceThread.start();
  }

  @Override
  public final void process(MRRecord rec) {
    try {
      recordsQueue.put(rec);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected final void onEndOfInput() {
    dumpThreadFull("endOfInput");
    super.onEndOfInput();
    try {
      try {
        recordsQueue.put(EOF);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      reduceThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract void reduce(String key, Iterator<MRRecord> reduce);
}
