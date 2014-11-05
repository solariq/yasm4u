package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import org.apache.commons.io.FileUtils;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.tables.FixedMRTable;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:34
 */
public class LocalMREnv implements MREnv {
  public static final String DEFAULT_HOME = System.getenv("HOME") + "/.MRSamples";
  private final String home;

  public static LocalMREnv createTemp() {
    try {
      final File temp = File.createTempFile("mrenv", "local");
      temp.delete();
      temp.mkdir();
      temp.deleteOnExit();
      return new LocalMREnv(temp.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  public LocalMREnv(final String home) {
    this.home = home;
  }

  public LocalMREnv() {
    this(DEFAULT_HOME);
  }

  public boolean execute(final Class<? extends MRRoutine> routineClass, MRState state, final MRTable[] in, final MRTable[] out, final MRErrorsHandler errorsHandler) {
    final List<MRTableShard> inputShards = new ArrayList<>();
    for(int i = 0; i < in.length; i++) {
      inputShards.addAll(Arrays.asList(shards(in[i])));
    }
    final List<MRTableShard> outputShards = new ArrayList<>();
    for(int i = 0; i < out.length; i++) {
      inputShards.addAll(Arrays.asList(shards(out[i])));
    }
    return execute(MRRoutine.class, state, inputShards.toArray(new MRTableShard[inputShards.size()]), outputShards.toArray(new MRTableShard[outputShards.size()]), errorsHandler);
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> exec, final MRState state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler)
  {
    final List<Writer> outputs = new ArrayList<>();
    final List<String> inputNames = new ArrayList<>(in.length);
    final List<File> inputFiles = new ArrayList<>(in.length);

    try {
      final Constructor<? extends MRRoutine> constructor = exec.getConstructor(String[].class, MROutput.class, MRState.class);
      for(int i = 0; i < in.length; i++) {
        inputNames.add(in[i].path());
        inputFiles.add(((LocalMRTableShard)in[i]).file());
      }
      for(int i = 0; i < out.length; i++) {
        outputs.add(new FileWriter(((LocalMRTableShard) out[i]).file()));
      }

      final MRRoutine routine = constructor.newInstance(inputNames.toArray(new String[inputNames.size()]), new MROutputImpl(outputs.toArray(new Writer[outputs.size()]), new MRErrorsHandler() {
        @Override
        public void error(final String type, final String cause, final String table, final CharSequence record) {
          throw new RuntimeException(table + "\t" + record + "\n\t" + type + "\t" + cause + "\t");
        }

        @Override
        public void error(final Throwable th, final String table, final CharSequence record) {
          throw new RuntimeException(table + "\t" + record, th);
        }
      }), state);

      for (int i = 0; i < inputFiles.size(); i++) {
        final File file = inputFiles.get(i);
        CharSeqTools.processLines(new FileReader(file), routine);
      }

    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      for (int i = 0; i < outputs.size(); i++) {
        try {
          outputs.get(i).close();
        } catch (IOException e) {
          // skip
        }
      }
    }
    return true;
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> exec, final MRState state, final MRTable in, final MRTable... out) {
    return execute(exec, state, new MRTable[]{in}, out, null);
  }

  @Override
  public LocalMRTableShard[] shards(final MRTable table) {
    final File tableFile = new File(home, table.name());
    try {
      FileUtils.forceMkdir(tableFile.getParentFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new LocalMRTableShard[]{new LocalMRTableShard(table.name(), table, tableFile)};
  }

  @Override
  public LocalMRTableShard resolve(final String path) {
    return new LocalMRTableShard(path, new FixedMRTable(path), new File(home, path + ".txt"));
  }

  @Override
  public MRTableShard restore(final String path, final long ts, final boolean available, final String crc) {
    return new LocalMRTableShard(path, new FixedMRTable(path), new File(home, path + ".txt"), ts, available, crc);
  }

  @Override
  public int read(final MRTableShard shard, final Processor<CharSequence> seq) {
    try {
      final int[] counter = new int[]{0};
      final File file = ((LocalMRTableShard) shard).file();
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
    final LocalMRTableShard localShard = (LocalMRTableShard) shard;
    try {
      StreamTools.transferData(content, new FileWriter(localShard.file()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    final LocalMRTableShard localShard = (LocalMRTableShard) shard;
    try {
      FileUtils.forceMkdir(((LocalMRTableShard) shard).file().getParentFile());
      StreamTools.transferData(content, new FileWriter(localShard.file(), true));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(final MRTableShard shard) {
    try {
      if (((LocalMRTableShard) shard).file().exists())
        FileUtils.forceDelete(((LocalMRTableShard) shard).file());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sample(final MRTableShard shard, final Processor<CharSequence> seq) {
    final Random rng = new FastRandom(shard.crc().hashCode());
    try {
      CharSeqTools.processLines(new FileReader(((LocalMRTableShard)shard).file()), new Processor<CharSequence>() {
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
  public void sort(final MRTableShard shard) {
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
    write(shard, new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", sorted)));
  }

  @Override
  public String name() {
    return "LocalMR://" + home;
  }

  public class LocalMRTableShard extends MRTableShard {
    private final File tableFile;

    private LocalMRTableShard(final String path, final MRTable owner, final File tableFile) {
      this(path, owner, tableFile, System.currentTimeMillis(), tableFile.exists(), tableFile.exists() ? "" + tableFile.length() : "");
    }

    public LocalMRTableShard(final String path, final MRTable table, final File file, final long ts, final boolean available, final String crc) {
      super(path, LocalMREnv.this, table, available, crc, ts);
      tableFile = file;
      try {
        FileUtils.forceMkdir(tableFile.getParentFile());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public File file() {
      return tableFile;
    }
  }
}
