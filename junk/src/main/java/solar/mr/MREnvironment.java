package solar.mr;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqComposite;
import com.spbsu.commons.seq.CharSeqTools;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public abstract class MREnvironment {
  private static final Logger LOG = Logger.getLogger(MREnvironment.class);

  public static final String FORBIDEN = MREnvironment.class.getName().replace('.', '/');

  private String mrServer = "cedar:8013";
  private String mrUser = "mobilesearch";
  private String mrSamplesDir = System.getenv("HOME") + "/.MRSamples";

  private Processor<CharSequence> outputProcessor = new Processor<CharSequence>() {
    @Override
    public void process(final CharSequence arg) {
      System.out.println(arg);
    }
  };
  private Processor<CharSequence> errorsProcessor = new Processor<CharSequence>() {
    @Override
    public void process(final CharSequence arg) {
      System.err.println(arg);
    }
  };

  protected abstract Process generateExecCommand(List<String> mrOptions);

  public void setOutputProcessor(final Processor<CharSequence> outputProcessor) {
    this.outputProcessor = outputProcessor;
  }

  public void setErrorsProcessor(final Processor<CharSequence> errorsProcessor) {
    this.errorsProcessor = errorsProcessor;
  }

  public void setMRServer(final String mrServer) {
    this.mrServer = mrServer;
  }

  public void setMRUser(final String mrUser) {
    this.mrUser = mrUser;
  }

  public void setMRSamplesDir(final String mrSamplesDir) {
    this.mrSamplesDir = mrSamplesDir;
  }

  public int read(MRTable table, final Processor<CharSequence> linesProcessor) {
    final int[] recordsCount = new int[]{0};
    final List<String> options = defaultOptions();
    for (int i = 0; i < table.length(); i++) {
      options.add("-read");
      options.add(table.at(i));
    }
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        recordsCount[0]++;
        linesProcessor.process(arg);
      }
    }, errorsProcessor);
    return recordsCount[0];
  }

  public void delete(final MRTable errorsTable) {
    final List<String> options = defaultOptions();

    for (int i = 0; i < errorsTable.length(); i++) {
      options.add("-drop");
      options.add(errorsTable.at(i));
      executeCommand(options, outputProcessor, errorsProcessor);
      options.remove(options.size() - 1);
    }
  }

  public int head(MRTable table, int count, final Processor<CharSequence> linesProcessor) {
    final List<String> options = defaultOptions();
    for (int i = 0; i < table.length(); i++) {
      options.add("-read");
      options.add(table.at(i));
    }
    options.add("-count");
    options.add("" + count);
    int[] recordsCount = new int[]{0};
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, errorsProcessor);
    return recordsCount[0];
  }

  private List<String> defaultOptions() {
    List<String> options = new ArrayList<>();
    { // access settings
      options.add("-server");
      options.add(mrServer);
      options.add("-opt");
      options.add("user=" + mrUser);
    }
    return options;
  }

  public boolean execute(final Class<? extends MRRoutine> routineClass, final MRTable table, final MRTable... output) {
    final FixedMRTable errorsTable = new FixedMRTable("tmp/errors-" + Integer.toHexString(new FastRandom().nextInt()));
    final File sampleFileOfType = new File(mrSamplesDir, table.name());

    try {
      if (!sampleFileOfType.exists()) {
        FileUtils.forceMkdir(sampleFileOfType.getParentFile());
        try (final FileWriter writer = new FileWriter(sampleFileOfType)) {
          head(table, 100, new Processor<CharSequence>() {
            @Override
            public void process(final CharSequence arg) {
              try {
                writer.append(arg).append("\n");
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<String> options = defaultOptions();
    int inputTablesCount = 0;
    final int outputTablesCount;
    { // sources/dst
      int outputTablesCounter = 0;
      for (int i = 0; i < table.length(); i++) {
        options.add("-src");
        options.add(table.at(i));
        inputTablesCount++;
      }
      for (int i = 0; i < output.length; i++) {
        MRTable mrTable = output[i];
        for (int j = 0; j < mrTable.length(); j++) {
          options.add("-dst");
          options.add(mrTable.at(j));
          outputTablesCounter++;
        }
      }
      options.add("-dst");
      options.add(errorsTable.at(0));
      outputTablesCounter++;
      outputTablesCount = outputTablesCounter;
    }

    final File tempFile;
    final PrintStream out = System.out;
    final PrintStream err = System.err;
    try {
      tempFile = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      tempFile.delete();
      tempFile.deleteOnExit();
      System.setOut(new PrintStream("/dev/null"));
      System.setErr(new PrintStream("/dev/null"));
      MRTools.buildClosureJar(MRRunner.class, tempFile.getAbsolutePath(), new Action<Class>() {
        @SuppressWarnings("unchecked")
        @Override
        public void invoke(final Class loadedClass) {
          final Constructor<?> constructor;
          try {
            constructor = loadedClass.getConstructor(char[].class, int.class, char[].class);
            final Object instance = constructor.newInstance(routineClass.getName().toCharArray(), outputTablesCount, sampleFileOfType.getAbsolutePath().toCharArray());
            loadedClass.getMethod("run").invoke(instance);
          } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      });

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      System.setOut(out);
      System.setErr(err);
    }
    try {
      options.add("-file");
      options.add(tempFile.toString());
      if (MRMap.class.isAssignableFrom(routineClass))
        options.add("-map");
      else if (MRReduce.class.isAssignableFrom(routineClass)) {
        options.add(inputTablesCount > 1 && inputTablesCount < 10 ? "-reducews" : "-reduce");
      } else
        throw new RuntimeException("Unknown MR routine type");
      options.add("java -jar " + tempFile.getName() + " " + routineClass.getName() + " " + outputTablesCount);
      executeCommand(options, outputProcessor, errorsProcessor);
      return read(errorsTable, new Processor<CharSequence>() {
        final FileWriter sampleFileWriter;
        {
          try {
            sampleFileWriter = new FileWriter(sampleFileOfType, true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        @Override
        public void process(final CharSequence arg) {
          final CharSequence[] parts = CharSeqTools.split(arg, '\t');
          if (parts.length < 3) {
            System.err.println(arg);
            return;
          }
          parts[1] = CharSeqTools.replace(parts[1], "\\n", "\n");
          parts[1] = CharSeqTools.replace(parts[1], "\\t", "\t");
          final CharSeqComposite data = new CharSeqComposite(parts, 2, parts.length);
          LOG.error(parts[1] + "\n on data:\n" + data);
          try {
            sampleFileWriter.append(data).append('\n');
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }) == 0;
    }
    finally {
      delete(errorsTable);
    }
  }

  private void executeCommand(final List<String> options, final Processor<CharSequence> outputProcessor, final Processor<CharSequence> errorsProcessor) {
    try {
      final Process exec = generateExecCommand(options);
      if (exec == null)
        return;

      final Thread outThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getInputStream(), Charset.forName("UTF-8")), outputProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      final Thread errThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getErrorStream(), Charset.forName("UTF-8")), errorsProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });

      outThread.start();
      errThread.start();
      exec.waitFor();
      outThread.join();
      errThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
