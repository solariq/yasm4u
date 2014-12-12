package solar.mr.env;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.impl.WeakListenerHolderImpl;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.JSONTools;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.MRWhiteboardImpl;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

import java.io.*;
import java.util.*;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YaMREnv extends WeakListenerHolderImpl<MREnv.ShardAlter> implements ProfilableMREnv {
  private final String user;

  private final String master;
  protected Processor<CharSequence> defaultErrorsProcessor;
  protected Processor<CharSequence> defaultOutputProcessor;
  private final ProcessRunner runner;
  private final ClosureJarBuilder jarBuilder;

  public YaMREnv(final ProcessRunner runner, final String user, final String master) {
    this(runner, user, master,
        new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            System.err.println(arg);
          }
        },
        new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            System.out.println(arg);
          }
        },
        new ClosureJarBuilder(LocalMREnv.DEFAULT_HOME)
    );
  }

  protected YaMREnv(final ProcessRunner runner, final String user, final String master,
                    final Processor<CharSequence> errorsProc,
                    final Processor<CharSequence> outputProc,
                    ClosureJarBuilder jarBuilder) {
    this.runner = runner;
    this.user = user;
    this.master = master;
    this.defaultErrorsProcessor = errorsProc;
    this.defaultOutputProcessor = outputProc;
    this.jarBuilder = jarBuilder;
  }

  protected List<String> defaultOptions() {
    final List<String> options = new ArrayList<>();
    { // access settings
      options.add("-subkey");
      options.add("-tableindex");
      options.add("-opt");
      options.add("user=" + user);
      options.add("-server");
      options.add(master);
    }
    return options;
  }

  public int read(MRTableShard shard, final Processor<CharSequence> linesProcessor) {
    final int[] recordsCount = new int[]{0};
    final List<String> options = defaultOptions();
    options.add("-read");
    options.add(localPath(shard));
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        recordsCount[0]++;
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
    return recordsCount[0];
  }

  public void sample(MRTableShard table, final Processor<CharSequence> linesProcessor) {
//    if (!table.isAvailable())
//      return;
//    final MRWhiteboard wb = new MRWhiteboardImpl(this, "sample", user);
//    wb.set("var:probability", Math.min(1., 1000. / table.keysCount()));
//    final MRTableShard shard = wb.get("temp:mr:///sample");
//    wb.remove("temp:mr:///sample");
//    MRState state = new MRStateImpl(wb);
//    execute(KeysSampleMap.class, state, new MRTableShard[]{table}, new MRTableShard[]{shard}, new MRErrorsHandler() {
//      @Override
//      public void error(String type, String cause, MRRecord record) {
//        throw new RuntimeException(record.toString() + "\n" + type + "\t" + cause.replace("\\t", "\t").replace("\\n", "\n"));
//      }
//      @Override
//      public void error(Throwable th, MRRecord record) {
//        throw new RuntimeException(record.toString(), th);
//      }
//    });
//    read(shard, linesProcessor);
//    delete(shard);

    final List<String> options = defaultOptions();
    options.add("-read");
    options.add(localPath(table));
    options.add("-count");
    options.add("" + 100);
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  @Override
  public MRTableShard[] list(String prefix) {
    final List<MRTableShard> result = new ArrayList<>();
    final List<String> options = defaultOptions();
    options.add("-list");
    options.add("-prefix");
    options.add(localPath(new MRWhiteboardImpl.LazyTableShard(prefix,this)));
    options.add("-jsonoutput");
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    }, defaultErrorsProcessor, null);
    final CharSeq build = builder.build();
    try {
      JsonParser parser = JSONTools.parseJSON(build);
      ObjectMapper mapper = new ObjectMapper();
      JsonToken next = parser.nextToken();
      assert JsonToken.START_ARRAY.equals(next);
      next = parser.nextToken();
      while (!JsonToken.END_ARRAY.equals(next)) {
        final JsonNode metaJSON = mapper.readTree(parser);
        final JsonNode nameNode = metaJSON.get("name");
        if (nameNode != null && !nameNode.isMissingNode()) {
          final String name = nameNode.textValue();
          final long size = metaJSON.get("full_size").longValue();
          final String sorted = metaJSON.has("sorted") ? metaJSON.get("sorted").toString() : "0";
          final long ts = metaJSON.has("mod_time") ? metaJSON.get("mod_time").longValue() : System.currentTimeMillis();
          final long recordsCount = metaJSON.has("records") ? metaJSON.get("records").longValue() : 0;
          result.add(new MRTableShard(name, this, true, "1".equals(sorted), "" + size, size, recordsCount/10, recordsCount, System.currentTimeMillis()));
        }
        next = parser.nextToken();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error parsing JSON from server: " + build, e);
    }
    for (int i = 0; i < result.size(); i++) {
      final MRTableShard shard = result.get(i);
      shardsCache.put(localPath(shard), shard);
    }

    return result.toArray(new MRTableShard[result.size()]);
  }

  @Override
  public void copy(MRTableShard[] from, MRTableShard to, boolean append) {
    final List<String> options = defaultOptions();
    long totalLength = append ? to.length() : 0;
    long recordsCount = append ? to.recordsCount() : 0;
    long keysCount = append ? to.keysCount() : 0;
    for(int i = 0; i < from.length; i++) {
      options.add("-src");
      options.add(localPath(from[i]));
      totalLength += from[i].length();
      recordsCount += from[i].recordsCount();
      keysCount += from[i].keysCount();
    }
    options.add(append ? "-dstappend" : "-dst");
    options.add(localPath(to));
    options.add("-copy");
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(
            new MRTableShard(localPath(to), to.container(), true, false, "" + totalLength, totalLength, keysCount, recordsCount, System.currentTimeMillis()),
            ShardAlter.AlterType.UPDATED));
  }

  public void write(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add(localPath(shard));
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    invoke(new ShardAlter(MRTools.updateTableShard(shard, false, cis), ShardAlter.AlterType.UPDATED));
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add("-dstappend");
    options.add(localPath(shard));
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), shard.recordsCount(), shard.keysCount(), shard.length());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    invoke(new ShardAlter(MRTools.updateTableShard(shard, false, cis), ShardAlter.AlterType.UPDATED));
  }

  private String localPath(MRTableShard shard) {
    if (shard.path().length() > 0 && shard.path().startsWith("/")) {
      return shard.path().substring(1);
    }
    return shard.path();
  }

  public void delete(final MRTableShard shard) {
    if (!shard.isAvailable())
      return;
    final List<String> options = defaultOptions();
    options.add("-drop");
    options.add(localPath(shard));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(
            new MRTableShard(localPath(shard), shard.container(), false, false, "0", 0, 0, 0, System.currentTimeMillis()),
            ShardAlter.AlterType.UPDATED));
  }

  public MRTableShard sort(final MRTableShard shard) {
    if (shard.isSorted())
      return shard;
    final List<String> options = defaultOptions();
    final MRTableShard newShard = new MRTableShard(localPath(shard), this, true, true, shard.crc(), shard.length(), shard.keysCount(), shard.recordsCount(), System.currentTimeMillis());
    options.add("-sort");
    options.add(localPath(shard));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
    invoke(new ShardAlter(newShard, ShardAlter.AlterType.UPDATED));
    return newShard;
  }

  @Override
  public String getTmp() {
    return "temp/";
  }

  private void executeCommand(final List<String> options, final Processor<CharSequence> outputProcessor,
                              final Processor<CharSequence> errorsProcessor, InputStream contents) {
    try {
      final Process exec = runner.start(options, contents);
      if (exec == null)
        return;
      final Thread outThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getInputStream(), StreamTools.UTF), outputProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      final Thread errThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getErrorStream(), StreamTools.UTF), errorsProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      exec.getOutputStream().close();
      outThread.start();
      errThread.start();
      exec.waitFor();
      outThread.join();
      errThread.join();
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MRTableShard resolve(final String path, Profiler profiler) {
    return resolveAll(new String[]{path}, profiler)[0];
  }

  @Override
  public MRTableShard resolve(final String path) {
    return resolveAll(new String[]{path}, EMPTY_PROFILER)[0];
  }

  private static final Set<String> FAT_DIRECTORIES = new HashSet<>(Arrays.asList(
          "user_sessions",
          "redir_log",
          "access_log",
          "reqans_log"
  ));

  private static Set<String> findBestPrefixes(Set<String> paths) {
    if (paths.size() < 2)
      return paths;
    final Set<String> result = new HashSet<>();
    final TObjectIntMap<String> parents = new TObjectIntHashMap<>();
    for(final String path : paths.toArray(new String[paths.size()])){
      int index = path.lastIndexOf('/');
      if (index < 0 || FAT_DIRECTORIES.contains(path.substring(0, index))) {
        result.add(path);
        paths.remove(path);
      }
      else parents.adjustOrPutValue(path.substring(0, index), 1, 1);
    }
    for(final String path : paths) {
      final String parent = path.substring(0, path.lastIndexOf('/'));

      if (parents.get(parent) == 1 && CharSeqTools.split(parent, '/').length < 2) {
        result.add(path);
        parents.remove(parent);
      }
    }
    result.addAll(findBestPrefixes(parents.keySet()));
    return result;
  }

  public MRTableShard[] resolveAll(String[] paths) {
    return resolveAll(paths, EMPTY_PROFILER);
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths, Profiler profiler) {
    final MRTableShard[] result = new MRTableShard[paths.length];
    final long time = System.currentTimeMillis();
    final Set<String> unknown = new HashSet<>();
    for(int i = 0; i < paths.length; i++) {
      final String path = paths[i];
      final MRTableShard shard = shardsCache.get(path);
      if (shard != null) {
        if (time - shard.metaTS() < MRTools.FRESHNESS_TIMEOUT) {
          result[i] = shard;
          continue;
        }
        shardsCache.clear(path);
      }
      unknown.add(path);
    }

    final Set<String> bestPrefixes = findBestPrefixes(unknown);
    for (final String prefix : bestPrefixes) {
      final MRTableShard[] list;
      if (profiler.isEnabled()) {
        list = profiler.getPofilableEnv().list(prefix);
      } else {
        list = list(prefix);
      }
      for(int i = 0; i < list.length; i++) {
        final MRTableShard shard = list[i];
        final int index = ArrayTools.indexOf(localPath(shard), paths);
        if (index >= 0)
          result[index] = shard;
      }
    }
    for(int i = 0; i < result.length; i++) {
      if (result[i] == null)
        result[i] = new MRTableShard(paths[i], this, false, false, "0", 0, 0, 0, time);
      invoke(new ShardAlter(result[i], ShardAlter.AlterType.UPDATED));
    }
    return result;
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> routineClass, final MRState state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler, final Profiler profiler)
  {
    final List<String> options = defaultOptions();

    int inputShardsCount = 0;
    int outputShardsCount = 0;
    for(int i = 0; i < in.length; i++) {
      options.add("-src");
      options.add(localPath(in[i]));
      inputShardsCount++;
    }
    for(int i = 0; i < out.length; i++) {
      options.add("-dst");
      options.add(localPath(out[i]));
      outputShardsCount++;
    }
    final String errorsShardName = "temp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    options.add("-dst");
    options.add(errorsShardName);
    if (profiler.isEnabled()) {
      options.add("-dst");
      options.add(profiler.getTableName());
    }
    final File jarFile;
    synchronized (jarBuilder) {
      for(int i = 0; i < in.length; i++) {
        jarBuilder.addInput(in[i]);
      }
      for(int i = 0; i < out.length; i++) {
        jarBuilder.addOutput(out[i]);
      }
      jarBuilder.setRoutine(routineClass);
      jarBuilder.setState(state);
      jarFile = jarBuilder.build();
      options.add("-file");
      options.add(jarFile.getAbsolutePath());
    }

    if (MRMap.class.isAssignableFrom(routineClass))
      options.add("-map");

    else if (MRReduce.class.isAssignableFrom(routineClass)) {
      options.add(inputShardsCount > 1 && inputShardsCount < 10 ? "-reducews" : "-reduce");
    } else
      throw new RuntimeException("Unknown MR routine type");
    options.add("java -XX:-UsePerfData -Xmx1G -Xms1G -jar " + jarFile.getName() + " " + routineClass.getName() + " " + outputShardsCount + " " + profiler.isEnabled());
    final int[] errorsCount = new int[]{0};
    executeCommand(options, defaultOutputProcessor, new Processor<CharSequence>() {
      String table;
      String key;
      String subkey;
      String value;
      @Override
      public void process(final CharSequence arg) {
        errorsCount[0]++;
        if (CharSeqTools.startsWith(arg, " table: ")) {
          table = arg.subSequence(" table: ".length(), arg.length()).toString();
        }
        else if (CharSeqTools.startsWith(arg, " key: ")) {
          key = arg.subSequence(" key: ".length(), arg.length()).toString();
        }
        else if (CharSeqTools.startsWith(arg, " subkey: ")) {
          subkey = arg.subSequence(" subkey: ".length(), arg.length()).toString();
        }
        else if (CharSeqTools.startsWith(arg, " value(p): ")) {
          value = arg.subSequence(" value(p): ".length(), arg.length()).toString();
          errorsHandler.error("MR exec error", "Who knows", new MRRecord(table, key, subkey, value));
        }
        System.err.println(arg);
      }
    }, null);
    final MRTableShard errorsShard = new MRTableShard(errorsShardName, this, true, false, "0", 0, 0, 0, System.currentTimeMillis());
    MRRoutine errorProcessor = new MRRoutine(new String[]{errorsShardName}, null, state) {
      @Override
      public void invoke(final MRRecord record) {
        CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[4]);
        errorsHandler.error(record.key, record.sub, new MRRecord(parts[0].toString(), parts[1].toString(), parts[2].toString(), parts[3]));
        System.err.println(record.value);
        System.err.println(record.key + "\t" + record.sub.replace("\\n", "\n").replace("\\t", "\t"));
      }
    };
    if (!profiler.isEnabled()) {
      errorsCount[0] += read(errorsShard, errorProcessor);
      delete(errorsShard);
    } else {
      errorsCount[0] += profiler.getPofilableEnv().read(errorsShard, errorProcessor);
      profiler.getPofilableEnv().delete(errorsShard);
      final MRTableShard profilerShard = new MRTableShard(profiler.getTableName(), this, true, false, "0", 0, 0, 0, System.currentTimeMillis());
      final Map<String, Integer> stat = new HashMap<>();
      profiler.getPofilableEnv().read(profilerShard, new MRRoutine(new String[]{profiler.getTableName()}, null, state) {
        @Override
        public void invoke(final MRRecord record) {
          stat.put(record.key, Integer.parseInt(record.value.toString()));
        }
      });
      profiler.addExecutionStatistics(stat);
      profiler.getPofilableEnv().delete(profilerShard);
    }

    if (errorsCount[0] == 0) {
      for(int i = 0; i < out.length; i++) {
        invoke(new ShardAlter(out[i]));
      }
    }
    return errorsCount[0] == 0;
  }

  @Override
  public String name() {
    return "YaMR://" + master + "/";
  }

  @Override
  public String toString() {
    return "YaMR://" + user + "@" + master + "/";
  }

  public ClosureJarBuilder getJarBuilder() {
    return jarBuilder;
  }

  @Override
  public boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTableShard[] in, MRTableShard[] out, MRErrorsHandler errorsHandler) {
    return execute(exec, state, in, out, errorsHandler, EMPTY_PROFILER);
  }

  private final FixedSizeCache<String, MRTableShard> shardsCache = new FixedSizeCache<>(1000, CacheStrategy.Type.LRU);

  @Override
  protected void invoke(ShardAlter e) {
    if (e.type == ShardAlter.AlterType.CHANGED) {
      shardsCache.clear(localPath(e.shard));
    }
    else if (e.type == ShardAlter.AlterType.UPDATED) {
      shardsCache.put(localPath(e.shard), e.shard);
    }
    super.invoke(e);
  }
}
