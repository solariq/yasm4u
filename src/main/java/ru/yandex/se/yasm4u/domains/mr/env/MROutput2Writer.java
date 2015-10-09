package ru.yandex.se.yasm4u.domains.mr.env;

import com.spbsu.commons.util.Pair;
import org.apache.log4j.Logger;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRReduce;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User: solar
 * Date: 29.01.15
 * Time: 19:44
 */
public class MROutput2Writer extends MROutputBase {
  private static final Pair<Integer, CharSequence> STOP = new Pair<Integer, CharSequence>(-1, "");
  private static Logger LOG = Logger.getLogger(MROutputBase.class);
  private final Thread outputThread;
  private final ArrayBlockingQueue<Pair<Integer, CharSequence>> queue = new ArrayBlockingQueue<Pair<Integer, CharSequence>>(MRReduce.MAX_REDUCE_SIZE);

  public MROutput2Writer(final Writer out, MRPath[] outputTables) {
    /* Builder were created on LocalEnv which doesn't use error table.
    On remote environment we append error table to the tail of destination tables. */
    super(outputTables);
    outputThread = new Thread(new Runnable() {
      @Override
      public void run() {
        int lastActiveTable = 0;
        try {
          try {
            //noinspection InfiniteLoopStatement
            Pair<Integer, CharSequence> next;
            while ((next = queue.take()) != STOP) {
              if (next.getFirst() != lastActiveTable) {
                out.append(next.getFirst().toString()).append('\n');
                lastActiveTable = next.getFirst();
              }
              out.append(next.getSecond()).append('\n');
            }
          }
          finally {
            out.close();
          }
        }
        catch (IOException | InterruptedException e) {
          LOG.error(e);
        }
        stopped = true;
      }
    }, "MR output thread");
    outputThread.setDaemon(true);
    outputThread.start();
  }

  public void join() {
    try {
      outputThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }


  private volatile boolean stopped = false;
  @Override
  protected void push(int tableNo, CharSequence record) {
    if (!stopped)
      try {
        queue.put(Pair.create(tableNo, record));
      }
      catch (InterruptedException ignored){}
  }

  @Override
  public void stop() {
    if (!stopped)
      try {
        queue.put(STOP);
      }
      catch (InterruptedException ignored){}
  }

  public void interrupt() {
    stop();
    stopped = true;
  }
}