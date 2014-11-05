package solar.mr;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.concurrent.LinkedTransferQueue;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Holder;
import com.spbsu.commons.util.Pair;
import org.apache.log4j.Logger;

/**
* User: solar
* Date: 23.09.14
* Time: 10:41
*/
public class MRRunner implements Runnable {
  public static final Pair<Integer, CharSequence> STOP = new Pair<Integer, CharSequence>(-1, "");
  private static Logger LOG = Logger.getLogger(MRRunner.class);
  private static final String SAMPLES_DIR = "~/.YaMRSamples/";
  private final MRRoutine realRoutine;
  private final MyMROutput out;
  private final Reader in;

  @SuppressWarnings("UnusedDeclaration")
  public MRRunner(char[] className, int tablesCount, char[] sampleFileName) throws IOException {
    this(className, tablesCount,
        new InputStreamReader(new FileInputStream(new String(sampleFileName)), Charset.forName("UTF-8")),
        new OutputStreamWriter(System.out, Charset.forName("UTF-8")));
  }

  public MRRunner(char[] className, int tablesCount, Reader in, Writer out) {
    this.in = in;
    this.out = new MyMROutput(out, tablesCount);
    final String mrClass = new String(className);
    try {
      final Class<?> aClass = Class.forName(mrClass);
      final Constructor<?> constructor = aClass.getConstructor(MROutput.class);
      constructor.setAccessible(true);
      final Object instance = constructor.newInstance(this.out);

      if (MRRoutine.class.isAssignableFrom(aClass)) {
        realRoutine = (MRRoutine)instance;
      }
      else {
        throw new IllegalArgumentException();
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    final Holder<CharSequence> contextHolder = new Holder<CharSequence>(new CharSeqAdapter(""));
    try {
      CharSeqTools.processLines(in, new Processor<CharSequence>() {
        @Override
        public void process(final CharSequence arg) {
          contextHolder.setValue(arg);
          realRoutine.invoke(new CharSeqAdapter(arg));
        }
      });
      realRoutine.invoke(CharSeq.EMPTY);
    }
    catch (Exception e) {
      out.error(e, contextHolder.getValue());
    }
    finally {
      out.interrupt();
    }
  }

  public static void main(String[] args) {
    final MRRunner runner = new MRRunner(args[0].toCharArray(), Integer.parseInt(args[1]),
        new InputStreamReader(System.in, Charset.forName("UTF-8")),
        new OutputStreamWriter(System.out, Charset.forName("UTF-8")));
    runner.run();
    runner.out.join();
  }

  private static class MyMROutput implements MROutput {
    private final LinkedTransferQueue<Pair<Integer, CharSequence>> queue = new LinkedTransferQueue<>();
    private final int errorTable;
    private final Thread outputThread;

    private MyMROutput(final Writer output, final int errorTable) {
      this.errorTable = errorTable;
      outputThread = new Thread(new Runnable() {
        @Override
        public void run() {
          int lastActiveTable = 1;
          try (final Writer out = output) {
            //noinspection InfiniteLoopStatement
            Pair<Integer, CharSequence> next;
            while ((next = queue.take()) != STOP) {
              if (next.getFirst() != lastActiveTable) {
                out.append(next.getFirst().toString()).append('\n');
                lastActiveTable = next.getFirst();
              }
              out.append(next.getSecond()).append('\n');
            }
          } catch (IOException e) {
            LOG.error(e);
          } catch (InterruptedException e) {
            // skip
          }
        }
      }, "MR output thread");
    }

    @Override
    public void add(final String key, final String subkey, final CharSequence value) {
      push(1, CharSeqTools.concatWithDelimeter("\t", key, subkey, value));
    }

    @Override
    public void add(final int tableNo, final String key, final String subkey, final CharSequence value) {
      push(tableNo, CharSeqTools.concatWithDelimeter("\t", key, subkey, value));
    }

    @Override
    public void error(final String type, final String cause, final CharSequence data) {
      push(errorTable, CharSeqTools.concatWithDelimeter("\t", type, encodeKey(cause), data));
    }

    private CharSequence encodeKey(final String cause) {
      return CharSeqTools.replace(CharSeqTools.replace(cause, "\n", "\\n"), "\t", "\\t");
    }

    @Override
    public void error(final Throwable th, CharSequence record) {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      th.printStackTrace(new PrintStream(out));
      error(th.getClass().getName(), out.toString(), CharSeqTools.replace(record, "\n", "\\n"));
    }

    private void push(int tableNo, CharSequence record) {
      if (!outputThread.isAlive() && !outputThread.isInterrupted())
        outputThread.start();
      queue.add(new Pair<>(tableNo, record));
    }

    public void interrupt() {
      queue.add(STOP);
    }

    public void join() {
      try {
        outputThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
