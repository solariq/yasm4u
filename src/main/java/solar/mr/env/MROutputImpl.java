package solar.mr.env;

import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.apache.log4j.Logger;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MROutput;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.concurrent.LinkedTransferQueue;

/**
* User: solar
* Date: 17.10.14
* Time: 10:36
*/
public class MROutputImpl implements MROutput {
  private static Logger LOG = Logger.getLogger(MROutputImpl.class);
  private final LinkedTransferQueue<Pair<Integer, CharSequence>> queue = new LinkedTransferQueue<>();
  private final String[] outTables;
  private final int errorTable;
  private final Thread outputThread;
  private final MRErrorsHandler errorsHandler;
  private int lastActiveTable = 0;

  public MROutputImpl(final Writer out, String[] outputTables) {
    this.outTables = outputTables;
    this.errorsHandler = null;
<<<<<<< HEAD
    this.errorTable = outputTables.length;
=======
    this.errorTable = outputTables.length - 1;
>>>>>>> Rework for jar generation, MREnv execution interface, introduced job builders
    outputThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          try {
            //noinspection InfiniteLoopStatement
            Pair<Integer, CharSequence> next;
            while ((next = queue.take()) != MRRunner.STOP) {
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

  public MROutputImpl(MREnv env, String[] output, MRErrorsHandler handler) {
    if (!(env instanceof LocalMREnv))
      throw new IllegalArgumentException("Not implemented for environments other than local");
    this.outTables = output;
    this.errorsHandler = handler;
    final Writer[] out = new Writer[output.length];
    for(int i = 0; i < out.length; i++) {
      try {
        out[i] = new FileWriter(((LocalMREnv) env).file(output[i], false));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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
  public String[] names() {
    return outTables;
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
    if (errorsHandler != null) {
      errorsHandler.error(th, rec);
    }
    else {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (final ObjectOutputStream dos = new ObjectOutputStream(out)) {
        dos.writeObject(th);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      error(th.getClass().getName(), CharSeqTools.toBase64(out.toByteArray()).toString(), rec);
    }
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

  public void parse(CharSequence arg) {
    final CharSequence[] split = CharSeqTools.split(arg, '\t');

    if (split.length == 1) {
      lastActiveTable = CharSeqTools.parseInt(split[0]);
    }
    else if (split.length >= 3) {
      if (lastActiveTable == errorTable) {
        final MRRecord rec = new MRRecord(split[2].toString(), split[3].toString(), split[4].toString(), split[5]);

        boolean isException = false;
        {
          final Class<?> aClass;
          try {
            aClass = Class.forName(split[0].toString());
            isException = Throwable.class.isAssignableFrom(aClass);
          } catch (ClassNotFoundException e) {
            //
          }
        }
        if (isException) {
          try (final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(CharSeqTools.parseBase64(split[1])))) {
            error((Throwable) is.readObject(), rec);
          } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
        else {
          error(split[0].toString(), split[1].toString(), rec);
        }
      }
      else push(lastActiveTable, arg);
    }
    else throw new IllegalArgumentException("Can not parse MRRecord from string: " + arg);
  }
}
