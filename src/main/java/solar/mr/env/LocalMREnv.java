package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import org.apache.commons.io.FileUtils;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.MRTableShard;
import solar.mr.routines.MRRecord;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:34
 */
public class LocalMREnv implements MREnv {
  public static final String DEFAULT_HOME = System.getenv("HOME") + "/.MRSamples";
  private final File home;

  public static LocalMREnv createTemp() {
    try {
      final File temp = File.createTempFile("mrenv", "local");
      //noinspection ResultOfMethodCallIgnored
      temp.delete();
      //noinspection ResultOfMethodCallIgnored
      temp.mkdir();
      temp.deleteOnExit();
      return new LocalMREnv(temp.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LocalMREnv(final String home) {
    this.home = new File(home);
    if (!this.home.isDirectory())
      //noinspection ResultOfMethodCallIgnored
      this.home.delete();
    if (!this.home.exists())
      try {
        FileUtils.forceMkdir(this.home);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
  }

  public LocalMREnv() {
    this(DEFAULT_HOME);
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> exec, final MRState state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler)
  {
    final List<Writer> outputs = new ArrayList<>();
    final List<String> inputNames = new ArrayList<>(in.length);
    final List<File> inputFiles = new ArrayList<>(in.length);
    final boolean hasErrors[] = new boolean[]{false};

    try {
      final Constructor<? extends MRRoutine> constructor = exec.getConstructor(String[].class, MROutput.class, MRState.class);
      for(int i = 0; i < in.length; i++) {
        inputNames.add(in[i].path());
        inputFiles.add(file(in[i].path(), in[i].isSorted()));
      }
      for(int i = 0; i < out.length; i++) {
        outputs.add(new FileWriter(file(out[i].path(), out[i].isSorted())));
      }

      final MROutputImpl mrOutput = new MROutputImpl(outputs.toArray(new Writer[outputs.size()]), new MRErrorsHandler() {
        @Override
        public void error(final String type, final String cause, MRRecord rec) {
          hasErrors[0] = true;
          throw new RuntimeException(rec.source + "\t" + rec.toString() + "\n\t" + type + "\t" + cause + "\t");
        }

        @Override
        public void error(final Throwable th, final MRRecord rec) {
          hasErrors[0] = true;
          throw new RuntimeException(rec.source + "\t" + rec.toString(), th);
        }
      });
      final MRRoutine routine = constructor.newInstance(inputNames.toArray(new String[inputNames.size()]), mrOutput, state);

      for (int i = 0; i < inputFiles.size(); i++) {
        final File file = inputFiles.get(i);
        CharSeqTools.processLines(new FileReader(file), routine);
      }
      routine.process(CharSeq.EMPTY);

      mrOutput.interrupt();
      mrOutput.join();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
      throw new RuntimeException(e);
    }
    return !hasErrors[0];
  }

  @Override
  public MRTableShard resolve(final String path) {
    {
      final File sortedFile = file(path, true);
      if (sortedFile.exists())
        return new MRTableShard(path, this, true, true, "" + sortedFile.length());
    }
    {
      final File unsortedFile = file(path, false);
      if (unsortedFile.exists())
        return new MRTableShard(path, this, true, false, "" + unsortedFile.length());
    }
    return new MRTableShard(path, this, false, false, "0");
  }

  @Override
  public int read(final MRTableShard shard, final Processor<CharSequence> seq) {
    try {
      final int[] counter = new int[]{0};
      final File file = file(shard.path(), shard.isSorted());
      if (file.exists())
        CharSeqTools.processLines(new FileReader(file), new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            counter[0]++;
            seq.process(arg);
          }
        });
      return counter[0];
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(final MRTableShard shard, final Reader content) {
    writeFile(content, file(shard.path(), shard.isSorted()), false);
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    writeFile(content, file(shard.path(), shard.isSorted()), true);
  }

  private void writeFile(final Reader content, final File file, final boolean append) {
    try {
      FileUtils.forceMkdir(file.getParentFile());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (final Writer out = new FileWriter(file, append)) {
      StreamTools.transferData(content, out);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(final MRTableShard shard) {
    try {
      final File sorted = file(shard.path(), true);
      if (sorted.exists())
        FileUtils.forceDelete(sorted);
      final File unsorted = file(shard.path(), false);
      if (unsorted.exists())
        FileUtils.forceDelete(unsorted);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sample(final MRTableShard shard, final Processor<CharSequence> seq) {
    final Random rng = new FastRandom(shard.crc().hashCode());
    try {
      CharSeqTools.processLines(new FileReader(file(shard.path(), shard.isSorted())), new Processor<CharSequence>() {
        @Override
        public void process(final CharSequence arg) {
          if (rng.nextDouble() > 0.9)
            seq.process(arg);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MRTableShard sort(final MRTableShard shard) {
    if (shard.isSorted())
      return shard;
    final TreeMap<String, CharSequence> sort = new TreeMap<>();
    read(shard, new Processor<CharSequence>() {
      private CharSequence[] result = new CharSequence[3];

      @Override
      public void process(final CharSequence arg) {
        final CharSequence[] split = CharSeqTools.split(arg, '\t', result);
        sort.put(split[0].toString() + '\t' + split[1].toString(), arg);
      }
    });

    final List<CharSequence> sorted = new ArrayList<>();
    for (Map.Entry<String, CharSequence> entry : sort.entrySet()) {
      sorted.add(entry.getValue());
    }

    writeFile(new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", sorted)), file(shard.path(), true), false);
    return resolve(shard.path());
  }

  @Override
  public String name() {
    return "LocalMR://" + home.getAbsolutePath();
  }

  public String home() {
    return home.getAbsolutePath();
  }

  public File file(final String path, boolean sorted) {
    return new File(home, path + (sorted ? ".txt.sorted" : ".txt"));
  }
}
