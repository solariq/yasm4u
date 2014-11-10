package solar.mr.env;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.concurrent.LinkedTransferQueue;


import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.apache.log4j.Logger;
import solar.mr.MRErrorsHandler;
import solar.mr.MROutput;
import solar.mr.routines.MRRecord;

/**
* User: solar
* Date: 17.10.14
* Time: 10:36
*/
public class MROutputImpl implements MROutput {
  private static Logger LOG = Logger.getLogger(MROutputImpl.class);
  private final LinkedTransferQueue<Pair<Integer, CharSequence>> queue = new LinkedTransferQueue<>();
  private final int errorTable;
  private final Thread outputThread;
  private final MRErrorsHandler errorsHandler;

  public MROutputImpl(final Writer out, final int errorTable) {
    this.errorsHandler = null;
    this.errorTable = errorTable;
    outputThread = new Thread(new Runnable() {
      @Override
      public void run() {
        int lastActiveTable = 0;
        try {
          try {
            synchronized (outputThread) {
              //noinspection InfiniteLoopStatement
              Pair<Integer, CharSequence> next;
              while ((next = queue.take()) != MRRunner.STOP) {
                if (next.getFirst() != lastActiveTable) {
                  out.append(next.getFirst().toString()).append('\n');
                  lastActiveTable = next.getFirst();
                }
                out.append(next.getSecond()).append('\n');
                out.flush();
              }
            }
          }
          finally {
            out.close();
          }
        }
        catch (IOException e) {
          LOG.error(e);
        }
        catch (InterruptedException e) {
          // skip
        }
      }
    }, "MR output thread");
    outputThread.start();
  }

  public MROutputImpl(final Writer[] out, final MRErrorsHandler errorsHandler) {
    this.errorsHandler = errorsHandler;
    errorTable = out.length;
    outputThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          //noinspection InfiniteLoopStatement
          try {
            Pair<Integer, CharSequence> next;
            while ((next = queue.take()) != MRRunner.STOP) {
              out[next.getFirst()].append(next.getSecond().toString()).append('\n');
            }
          }
          finally {
            for(int i = 0; i < out.length; i++) {
              out[i].close();
            }
          }
        } catch (IOException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          // skip
        }
      }
    }, "MR output thread");
    outputThread.start();
  }

  @Override
  public void add(final String key, final String subkey, final CharSequence value) {
    add(0, key, subkey, value);
  }

  @Override
  public void add(final int tableNo, final String key, final String subkey, final CharSequence value) {
    if (tableNo > errorTable)
      throw new IllegalArgumentException("Incorrect table index: " + tableNo);
    if (tableNo == errorTable)
      throw new IllegalArgumentException("Errors table #" + tableNo + " must be accessed via error subroutines of outputter");
    if (key.isEmpty())
      throw new IllegalArgumentException("Key must be non empty!");
    if (key.getBytes().length > 4096)
      throw new IllegalArgumentException("Key must not exceed 4096 byte length!");
    if (subkey.isEmpty())
      throw new IllegalArgumentException("Subkey must be non empty");
    if (subkey.getBytes().length > 4096)
      throw new IllegalArgumentException("Subkey must not exceed 4096 byte length!");
    if (CharSeqTools.indexOf(value, "\n") >= 0)
      throw new IllegalArgumentException("Value can not contain \\n symbols for stream usage");

    push(tableNo, CharSeqTools.concatWithDelimeter("\t", key, subkey, value));
  }

  @Override
  public void error(final String type, final String cause, final MRRecord rec) {
    if (errorsHandler != null)
      errorsHandler.error(type, cause, rec);
    else
      push(errorTable, CharSeqTools.concatWithDelimeter("\t", type, encodeKey(cause), rec.source, rec.toString()));
  }

  private CharSequence encodeKey(final String cause) {
    return CharSeqTools.replace(CharSeqTools.replace(cause, "\n", "\\n"), "\t", "\\t");
  }

  @Override
  public void error(final Throwable th, final MRRecord rec) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    th.printStackTrace(new PrintStream(out));
    error(th.getClass().getName(), out.toString().replace("\n", "\\n"), rec);
  }

  private void push(int tableNo, CharSequence record) {
    if (!outputThread.isAlive() && !outputThread.isInterrupted())
      outputThread.start();
    queue.add(new Pair<>(tableNo, record));
  }

  public void interrupt() {
    queue.add(MRRunner.STOP);
  }

  public void join() {
    try {
      outputThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
