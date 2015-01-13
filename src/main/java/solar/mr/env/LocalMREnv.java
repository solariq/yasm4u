package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.impl.WeakListenerHolderImpl;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import solar.mr.*;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.*;

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
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler)
  {
    final boolean hasErrors[] = new boolean[]{false};

    try {
      final MROutputImpl output = new MROutputImpl(this, builder.output(), new MRErrorsHandler() {
        @Override
        public void error(final String type, final String cause, MRRecord rec) {
          hasErrors[0] = true;
          errorsHandler.error(type, cause, rec);
        }

        @Override
        public void error(final Throwable th, final MRRecord rec) {
          hasErrors[0] = true;
          errorsHandler.error(th, rec);
        }
      });
      final MRRoutine routine = builder.build(output);

      for (final String path : routine.input()) {
        final File file = file(path);
        if (file.exists())
          CharSeqTools.processLines(new FileReader(file), routine);
      }
      routine.process(CharSeq.EMPTY);

      output.interrupt();
      output.join();

      { // invalidating caches
        final long time = System.currentTimeMillis();
        for (MRTableShard anOut : resolveAll(routine.output())) {
          if (anOut.metaTS() < time)
            invoke(new ShardAlter(anOut, ShardAlter.AlterType.CHANGED));
        }
      }

    } catch (IOException e) {
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
        result = new MRTableShard(path, true, true, crc(sortedFile), length(sortedFile), recordsAndKeys[1], recordsAndKeys[0], System.currentTimeMillis());
      }
    }
    if (result == null) {
      final File unsortedFile = file(path, false);
      if (unsortedFile.exists()) {
        final long[] recordsAndKeys = countRecordsAndKeys(unsortedFile);
        result = new MRTableShard(path, true, false, crc(unsortedFile), length(unsortedFile), recordsAndKeys[1], recordsAndKeys[0], System.currentTimeMillis());
      }
    }
    if (result == null)
      result = new MRTableShard(path, false, false, "0", 0, 0, 0, System.currentTimeMillis());
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
  public MRTableShard write(final MRTableShard shard, final Reader content) {
    final MRTableShard updatedShard = writeFile(content, shard, false, false);
    invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    return updatedShard;
  }

  @Override
  public MRTableShard append(final MRTableShard shard, final Reader content) {
    final MRTableShard updatedShard = writeFile(content, shard, false, true);
    invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    return updatedShard;
  }

  private MRTableShard writeFile(final Reader content, final MRTableShard shard, boolean sorted, final boolean append) {
    final File file = file(shard.path(), sorted);
    try {
      FileUtils.forceMkdir(file.getParentFile());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final MRTools.CounterInputStream cis = append
            ? new MRTools.CounterInputStream(new LineNumberReader(content), shard.recordsCount(), shard.keysCount(), shard.length())
            : new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    try (final OutputStream out = new FileOutputStream(file, append)) {
      StreamTools.transferData(cis, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (!sorted)
        //noinspection ResultOfMethodCallIgnored
        file(shard.path(), true).delete();
    }
    return MRTools.updateTableShard(shard, sorted, cis);
  }

  @Override
  public MRTableShard delete(final MRTableShard shard) {
    final MRTableShard updatedShard = new MRTableShard(shard.path(), false, false, "0", 0, 0, 0, System.currentTimeMillis());
    try {
      final File sorted = file(shard.path(), true);
      if (sorted.exists())
        FileUtils.forceDelete(sorted);
      final File unsorted = file(shard.path(), false);
      if (unsorted.exists())
        FileUtils.forceDelete(unsorted);
      return updatedShard;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    }
  }

  @Override
  public void sample(final MRTableShard shard, final Processor<CharSequence> seq) {
    // the table could be empty, so no sample for such a table
    if (shard.isAvailable()) {
      try {
        CharSeqTools.processLines(new FileReader(file(shard.path(), shard.isSorted())), new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            seq.process(arg);
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public MRTableShard[] list(String prefix) {
    File prefixFile = new File(home, prefix);
    String startsWith = "";
    if (!prefixFile.exists()) {
      prefixFile = prefixFile.getParentFile();
      final String parentPrefix = prefixFile.getAbsolutePath().substring(home.getAbsolutePath().length() + 1);
      startsWith = prefix.substring(parentPrefix.length());
      prefix = parentPrefix;
    }
    final String finalPrefix = prefix;
    final HashMap<String, MRTableShard> result = new HashMap<>();
    final File finalPrefixFile = prefixFile;
    final String finalStartsWith = startsWith;
    StreamTools.visitFiles(prefixFile, new Processor<String>() {
      @Override
      public void process(String path) {
        if (!path.startsWith(finalStartsWith))
          return;
        final File file = new File(finalPrefixFile, path);

        if (path.endsWith(".txt")) {
          final long[] recordsAndKeys = countRecordsAndKeys(file);
          final String shardPath = finalPrefix + path.substring(0, path.indexOf(".txt"));
          if (!result.containsKey(shardPath)) {
            result.put(shardPath, new MRTableShard(shardPath, true, false, crc(file), length(file), recordsAndKeys[1], recordsAndKeys[0], System.currentTimeMillis()));
          }
        } else if (path.endsWith(".txt.sorted")) {
          final long[] recordsAndKeys = countRecordsAndKeys(file);
          final String shardPath = finalPrefix + path.substring(0, path.indexOf(".txt.sorted"));
          result.put(shardPath, new MRTableShard(shardPath, true, true, crc(file), length(file), recordsAndKeys[1], recordsAndKeys[0], System.currentTimeMillis()));
        }
      }
    });
    return result.values().toArray(new MRTableShard[result.size()]);
  }

  @Override
  public MRTableShard copy(MRTableShard[] from, MRTableShard to, boolean append) {
    try {
      for(int i = 0; i < from.length; i++) {
        final Reader content = from[i].isAvailable() ? new FileReader(file(from[i].path(), false)) : new CharSeqReader("");
        to = writeFile(content, to, false, append || i > 0);
      }
      invoke(new ShardAlter(to, ShardAlter.AlterType.UPDATED));
      return to;
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

    final CharSeqReader content = new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", sorted));
    invoke(new ShardAlter(writeFile(content, shard, true, false), ShardAlter.AlterType.UPDATED));
    return resolve(shard.path());
  }

  @Override
  public String getTmp() {
    return "temp/";
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

  public File file(final String path) {
    final File result = file(path, true);
    if (result != null && result.exists())
      return result;
    return file(path, false);
  }

  private String crc(File file) {
    return "" + file.length();
  }

  private long length(File file) {
    return file.length();
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
