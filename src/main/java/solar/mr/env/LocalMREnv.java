package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.impl.WeakListenerHolderImpl;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
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
public class LocalMREnv extends WeakListenerHolderImpl<MREnv.ShardAlter> implements MREnv{
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
      for (MRTableShard anIn : in) {
        inputNames.add(anIn.path());
        inputFiles.add(file(anIn.path(), anIn.isSorted()));
      }
      for (MRTableShard anOut : out) {
        outputs.add(new FileWriter(file(anOut.path(), false)));
        //noinspection ResultOfMethodCallIgnored
        file(anOut.path(), true).delete();
      }

      final MROutputImpl mrOutput = new MROutputImpl(outputs.toArray(new Writer[outputs.size()]), new MRErrorsHandler() {
        @Override
        public void error(final String type, final String cause, MRRecord rec) {
          hasErrors[0] = true;
          throw new RuntimeException(rec.source + "\t" + rec.toString() + "\n\t" + type + "\t" + cause.replace("\\n", "\n") + "\t");
        }

        @Override
        public void error(final Throwable th, final MRRecord rec) {
          hasErrors[0] = true;
          throw new RuntimeException(rec.source + "\t" + rec.toString(), th);
        }
      });
      final MRRoutine routine = constructor.newInstance(inputNames.toArray(new String[inputNames.size()]), mrOutput, state);

      for (final File file : inputFiles) {
        if (file.exists())
          CharSeqTools.processLines(new FileReader(file), routine);
      }
      routine.process(CharSeq.EMPTY);

      for (MRTableShard anOut : out) {
        invoke(new ShardAlter(anOut));
      }

      mrOutput.interrupt();
      mrOutput.join();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
      throw new RuntimeException(e);
    }
    return !hasErrors[0];
  }

  @Override
  public MRTableShard resolve(String path) {
    MRTableShard result = null;
    {
      final File sortedFile = file(path, true);
      if (sortedFile.exists()) {
        final long[] recordsAndKeys = countRecordsAndKeys(sortedFile);
        result = new MRTableShard(path, this, true, true, crc(sortedFile), length(sortedFile), recordsAndKeys[1], recordsAndKeys[0], ts(sortedFile));
      }
    }
    if (result == null) {
      final File unsortedFile = file(path, false);
      if (unsortedFile.exists()) {
        final long[] recordsAndKeys = countRecordsAndKeys(unsortedFile);
        result = new MRTableShard(path, this, true, false, crc(unsortedFile), length(unsortedFile), recordsAndKeys[1], recordsAndKeys[0], ts(unsortedFile));
      }
    }
    if (result == null)
      result = new MRTableShard(path, this, false, false, "0", 0, 0, 0, System.currentTimeMillis());
    invoke(new ShardAlter(result, ShardAlter.AlterType.UPDATED));
    return result;
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths) {
    final MRTableShard[] result = new MRTableShard[paths.length];
    for(int i = 0; i < result.length; i++) {
      result[i] = resolve(paths[i]);
    }
    return result;
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
    writeFile(content, shard.path(), false, false);
    invoke(new ShardAlter(shard));
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    writeFile(content, shard.path(), false, true);
    invoke(new ShardAlter(shard));
  }

  private void writeFile(final Reader content, final String path, boolean sorted, final boolean append) {
    File file = file(path, sorted);
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
    finally {
      if (!sorted)
        //noinspection ResultOfMethodCallIgnored
        file(path, true).delete();
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
    finally {
      invoke(new ShardAlter(shard));
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
  public MRTableShard[] list(String prefix) {
    File prefixFile = new File(home, prefix);
    if (!prefixFile.exists()) {
      prefixFile = prefixFile.getParentFile();
      prefix = prefix.substring(prefix.length() + 1 + home.getAbsolutePath().length() - prefixFile.getAbsolutePath().length());
    }
    final String finalPrefix = prefix;
    final List<MRTableShard> result = new ArrayList<>();
    final File finalPrefixFile = prefixFile;
    StreamTools.visitFiles(prefixFile, new Processor<String>(){
      @Override
      public void process(String path) {
        if (path.startsWith(finalPrefix)) {
          final File file = new File(finalPrefixFile, path);

          if (path.endsWith(".txt")) {
            final long[] recordsAndKeys = countRecordsAndKeys(file);
            result.add(new MRTableShard(path.substring(0, ".txt".length()), LocalMREnv.this, true, false, crc(file), length(file), recordsAndKeys[1], recordsAndKeys[0], ts(file)));
          } else if (path.endsWith(".txt.sorted")) {
            final long[] recordsAndKeys = countRecordsAndKeys(file);
            result.add(new MRTableShard(path.substring(0, ".txt.sorted".length()), LocalMREnv.this, true, true, crc(file), length(file), recordsAndKeys[1], recordsAndKeys[0], ts(file)));
          }
        }
      }
    });
    return result.toArray(new MRTableShard[result.size()]);
  }

  @Override
  public void copy(MRTableShard from, MRTableShard to, boolean append) {
    try {
      writeFile(from.isAvailable() ? new FileReader(file(from.path(), false)) : new CharSeqReader(""), to.path(), false, append);
      invoke(new ShardAlter(to));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MRTableShard sort(final MRTableShard shard) {
    if (shard.isSorted())
      return shard;
    final List<Pair<String, CharSequence>> sort = new ArrayList<>();
    read(shard, new Processor<CharSequence>() {
      private CharSequence[] result = new CharSequence[3];

      @Override
      public void process(final CharSequence arg) {
        final CharSequence[] split = CharSeqTools.split(arg, '\t', result);
        sort.add(Pair.create(split[0].toString() + '\t' + split[1].toString(), arg));
      }
    });
    Collections.sort(sort, new Comparator<Pair<String, CharSequence>>() {
      @Override
      public int compare(Pair<String, CharSequence> o1, Pair<String, CharSequence> o2) {
        return o1.getFirst().compareTo(o2.getFirst());
      }
    });
    final List<CharSequence> sorted = new ArrayList<>();
    for (Pair<String, CharSequence> entry : sort) {
      sorted.add(entry.getSecond());
    }
    sorted.add(CharSeqTools.EMPTY);

    writeFile(new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", sorted)), shard.path(), true, false);
    invoke(new ShardAlter(shard));
    return resolve(shard.path());
  }

  @Override
  public String name() {
    return "LocalMR://" + home.getAbsolutePath() + "/";
  }

  public String home() {
    return home.getAbsolutePath();
  }

  public File file(final String path, boolean sorted) {
    final File file = new File(home, path + (sorted ? ".txt.sorted" : ".txt"));
    if (!file.getParentFile().exists()) {
      try {
        FileUtils.forceMkdir(file.getParentFile());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return file;
  }

  private String crc(File file) {
    return "" + file.length();
  }

  private long length(File file) {
    return file.length();
  }

  private long ts(File sortedFile) {
    return sortedFile.lastModified();
  }

  private long[] countRecordsAndKeys(File file) {
    final long[] recordsAnsKeys = {0, 0};
    if (!file.exists()) {
      return recordsAnsKeys;
    }
    final Set<String> keys = new HashSet<>();
    try {
      StreamTools.readFile(file, new Processor<CharSequence>() {
        CharSequence[] parts = new CharSequence[2];
        @Override
        public void process(CharSequence arg) {
          parts = CharSeqTools.split(arg, '\t', parts);
          keys.add(parts[0].toString());
          recordsAnsKeys[0]++;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    recordsAnsKeys[1] = keys.size();
    return recordsAnsKeys;
  }
}
