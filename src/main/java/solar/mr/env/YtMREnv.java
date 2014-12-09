package solar.mr.env;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Action;
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
import org.apache.commons.lang3.NotImplementedException;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.MRTableShard;
import solar.mr.proc.impl.MRWhiteboardImpl;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends WeakListenerHolderImpl<MREnv.ShardAlter> implements ProfilableMREnv {
  private final String tag;
  private final String master;
  protected Processor<CharSequence> defaultErrorsProcessor;
  protected Processor<CharSequence> defaultOutputProcessor;
  private final ProcessRunner runner;
  private final ClosureJarBuilder jarBuilder;

  public YtMREnv(final ProcessRunner runner, final String tag, final String master) {
    this(runner, tag, master,
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

  protected YtMREnv(final ProcessRunner runner, final String tag, final String master,
                    final Processor<CharSequence> errorsProc,
                    final Processor<CharSequence> outputProc,
                    ClosureJarBuilder jarBuilder) {
    this.runner = runner;
    this.tag = tag;
    this.master = master;
    this.defaultErrorsProcessor = errorsProc;
    this.defaultOutputProcessor = outputProc;
    this.jarBuilder = jarBuilder;
  }

  protected List<String> defaultOptions() {
    final List<String> options = new ArrayList<>();
    { // access settings
      //options.add("-subkey");
      //options.add("-tableindex");
      options.add("--proxy");
      options.add(master);
    }
    return options;
  }

  public int read(MRTableShard shard, final Processor<CharSequence> linesProcessor) {
    final int[] recordsCount = new int[]{0};

    if (!shard.isAvailable())
      return 0;

    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>\"yamr");
    options.add(shard.path());
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
    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(table.path() + "[:#100]");
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  private static final Set<String> FAT_DIRECTORIES = new HashSet<>(Arrays.asList(
      "//userdata/user_sessions"
      //"redir_log",
      //"access_log",
      //"reqans_log"
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

  @Override
  public boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTableShard[] in, MRTableShard[] out, MRErrorsHandler errorsHandler) {
    return execute(exec, state, in, out, errorsHandler, EMPTY_PROFILER);
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths, final Profiler profiler) {
    final MRTableShard[] result = new MRTableShard[paths.length];
    final Set<String> bestPrefixes = findBestPrefixes(new HashSet<>(Arrays.asList(paths)));
    for (final String prefix : bestPrefixes) {
      final MRTableShard[] list = list(prefix);
      for(int i = 0; i < list.length; i++) {
        final MRTableShard shard = list[i];
        final int index = ArrayTools.indexOf(shard.path(), paths);
        if (index >= 0)
          result[index] = shard;
      }
    }
    for(int i = 0; i < result.length; i++) {
      if (result[i] == null)
        result[i] = new MRTableShard(paths[i], this, false, false, "0", 0, 0, 0, System.currentTimeMillis());
      invoke(new ShardAlter(result[i], ShardAlter.AlterType.UPDATED));
    }
    return result;
  }

  @Override
  public MRTableShard[] list(String prefix) {
    final List<String> defaultOptionsEntity = defaultOptions();
    defaultOptionsEntity.add("get");
    defaultOptionsEntity.add("--format");
    defaultOptionsEntity.add("json");

    final List<String> optionEntity = new ArrayList<>();
    optionEntity.addAll(defaultOptionsEntity);
    optionEntity.add(prefix + "/@");
    final AppenderProcessor getBuilder = new AppenderProcessor();
    executeCommand(optionEntity, getBuilder, defaultErrorsProcessor, null);

    final List<MRTableShard> result = new ArrayList<>();
    try {
      JsonParser getParser = JSONTools.parseJSON(getBuilder.sequence());
      extractTableFromJson(prefix, result, getParser);
      if (!result.isEmpty()) {
        return result.toArray(new MRTableShard[1]);
      }
    } catch (IOException| ParseException e) {
      return new MRTableShard[0];
    }

    final List<String> options = defaultOptions();
    options.add("list");
    options.add(prefix);
    final AppenderProcessor builder = new AppenderProcessor(" ");
    executeCommand(options, builder, defaultErrorsProcessor, null);

    final CharSequence[] listSeq = CharSeqTools.split(builder.trimmedSequence(), ' ');

    /* uber-for */
    final int[] index = new int[2];
    final CharSeqBuilder jsonOutputBuilder = new CharSeqBuilder();
    SepInLoopProcessor processor = new SepInLoopProcessor(jsonOutputBuilder, index, ',');
    jsonOutputBuilder.append('[');

    index[1] = listSeq.length;
    for (index[0] = 0; index[0] < listSeq.length; index[0] += 1) {
      result.add(new MRWhiteboardImpl.LazyTableShard(prefix
          + (index[0] != index[1] ? "/" + listSeq[index[0]].toString() : ""), this));
    }
    return result.toArray(new MRTableShard[result.size()]);
  }

  private void extractTableFromJson(final String prefix, List<MRTableShard> result, JsonParser parser) throws IOException, ParseException {
    final Calendar c = Calendar.getInstance();
    final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
    final ObjectMapper mapper = new ObjectMapper();

    final JsonNode metaJSON = mapper.readTree(parser);
    final JsonNode typeNode = metaJSON.get("type");
    if (typeNode != null && !typeNode.isMissingNode() && typeNode.textValue().equals("table"))  {
      final String name = metaJSON.get("key").asText(); /* it's a name in Yt */
      final long size = metaJSON.get("uncompressed_data_size").longValue();
      boolean sorted= metaJSON.has("sorted");
      c.setTime(formater.parse(metaJSON.get("modification_time").asText()));
      final long ts = c.getTimeInMillis();
      final long recordsCount = metaJSON.has("row_count") ? metaJSON.get("row_count").longValue() : 0;
      result.add(new MRTableShard(prefix.endsWith("/" + name)? prefix : prefix + "/" + name, this, true, sorted, "" + size, size, recordsCount/10, recordsCount, ts));
    }
  }

  @Override
  public void copy(MRTableShard[] from, MRTableShard to, boolean append) {
     throw new NotImplementedException("YtMRenv::copy");
  }

  public void write(final MRTableShard shard, final Reader content) {
    createTable(shard.path());
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(shard.path());
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    invoke(new ShardAlter(MRTools.updateTableShard(shard, false, cis), ShardAlter.AlterType.UPDATED));
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    createTable(shard.path());
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add("\"<append=true>" + shard.path() + "\"");
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), shard.recordsCount(), shard.keysCount(), shard.length());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    invoke(new ShardAlter(MRTools.updateTableShard(shard, false, cis), ShardAlter.AlterType.UPDATED));
  }

  private MRTableShard createTable(final String tableName) {
    final List<String> options = defaultOptions();
    options.add("create");
    options.add("-r");
    options.add("table");
    options.add(tableName);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    return resolve(tableName);
  }

  public void delete(final MRTableShard table) {
    final List<String> options = defaultOptions();

    options.add("remove");
    options.add(table.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(table));
  }

  public MRTableShard sort(final MRTableShard table) {
    throw new NotImplementedException("YtMREnv::sort");
  }

  @Override
  public String getTmp() {
    return "//tmp/";
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

  @Override
  public MRTableShard[] resolveAll(String[] strings) {
    return new MRTableShard[0];
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> routineClass, final MRState state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler, Profiler profiler)
  {
    final List<String> options = defaultOptions();
    if (MRMap.class.isAssignableFrom(routineClass))
      options.add("map");
    else if (MRReduce.class.isAssignableFrom(routineClass)) {
      //options.add(inputShardsCount > 1 && inputShardsCount < 10 ? "-reducews" : "-reduce");
      options.add("reduce");
    } else
      throw new RuntimeException("Unknown MR routine type");

    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");

    final MRTableShard[] realOut = new MRTableShard[out.length];
    for(int i = 0; i < out.length; i++) {
      options.add("--dst");
      options.add(out[i].path());
      realOut[i] = createTable(out[i].path()); /* lazy materialization */
    }

    final File jarFile;
    synchronized (jarBuilder) {
      for(int i = 0; i < in.length; i++) {
        jarBuilder.addInput(in[i]);
      }
      for(int i = 0; i < out.length; i++) {
        jarBuilder.addOutput(realOut[i]);
      }
      jarBuilder.setRoutine(routineClass);
      jarBuilder.setState(state);
      jarFile = jarBuilder.build();
      options.add("--local-file");
      options.add(jarFile.getAbsolutePath());
    }

    options.add("--memory-limit 2000");
    options.add("'/usr/local/java8/bin/java -XX:-UsePerfData -Xmx1G -Xms1G -jar " + jarFile.getName() + " " + routineClass.getName() + " " + out.length + "" + profiler.isEnabled() + "'");
    for(int i = 0; i < in.length; i++) {
      options.add("--src");
      options.add(in[i].path());
    }

    final String errorsShardName = "//tmp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    final MRTableShard errorsShard = createTable(errorsShardName);
    options.add("--dst");
    options.add(errorsShardName);

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
    errorsCount[0] += read(errorsShard, new MRRoutine(new String[]{errorsShardName}, null, state) {
      @Override
      public void invoke(final MRRecord record) {
        CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[4]);
        errorsHandler.error(record.key, record.sub, new MRRecord(parts[0].toString(), parts[1].toString(), parts[2].toString(), parts[3]));
        System.err.println(record.value);
        System.err.println(record.key + "\t" + record.sub.replace("\\n", "\n").replace("\\t", "\t"));
      }
    });
    delete(errorsShard);

    return errorsCount[0] == 0;
  }

  @Override
  public String name() {
    return "YaMR://" + master + "/";
  }

  @Override
  public String toString() {
    return "YaMR://" + tag + "@" + master + "/";
  }

  public ClosureJarBuilder getJarBuilder() {
    return jarBuilder;
  }

  private boolean isShardPathExists(final String path) {
    String shardName = path;
    final List<String> options = defaultOptions();
    options.add("exists");
    options.add(shardName);
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    }, defaultErrorsProcessor, null);

    return CharSeqTools.parseBoolean(builder.build());
  }

  boolean isShardPathSorted(final String path) {
    String shardName = path;
    final List<String> options = defaultOptions();
    options.add("get");
    options.add(shardName + "/@sorted");
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    }, defaultErrorsProcessor, null);

    CharSeq jsonBool = builder.build();
    return CharSeqTools.parseBoolean(jsonBool.subSequence(1, jsonBool.length() - 1));
  }

  private final FixedSizeCache<String, MRTableShard> shardsCache = new FixedSizeCache<>(1000, CacheStrategy.Type.LRU);

  @Override
  protected void invoke(ShardAlter e) {
    if (e.type == ShardAlter.AlterType.CHANGED) {
      shardsCache.clear(e.shard.path());
    }
    else if (e.type == ShardAlter.AlterType.UPDATED) {
      shardsCache.put(e.shard.path(), e.shard);
    }
    super.invoke(e);
  }

  private String upload(final String from, final String to) {

    final String toPath = "//tmp/" + tag + "/state/files";
    if (!isShardPathExists(toPath)) {
      createNode(toPath);
    }

    final String toRemoteFile = toPath + "/" + to;
    try {
      final List<String> options = defaultOptions();
      options.add("upload");
      options.add(toRemoteFile);
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, new FileInputStream(from));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return toRemoteFile;
  }

  private void createNode(String toPath) {
    final List<String> options = defaultOptions();
    options.add("create");
    options.add("map_node");
    options.add(toPath);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
  }

  private void deleteFile(final String file) {
    final List<String>  options = defaultOptions();
    options.add("remove");
    options.add(file);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
  }

  private static class SepInLoopProcessor implements Processor<CharSequence> {
    final CharSeqBuilder builder;
    final int[] indexAndLimit;
    final char sep;

    SepInLoopProcessor(final CharSeqBuilder builder, final int indexAndLimit[], char sep) {
      this.builder = builder;
      this.indexAndLimit = indexAndLimit;
      this.sep = sep;

    }
    @Override
    public void process(CharSequence arg) {
      builder.append(arg);
      if (indexAndLimit[0] < indexAndLimit[1] - 1)
        builder.append(sep);
    }
  }

  private static class AppenderProcessor implements Processor<CharSequence> {
    private final CharSeqBuilder builder;
    private final String sep;
    private boolean withSeparator;

    private AppenderProcessor(final String sep, boolean withSeparator) {
      this.sep = sep;
      this.withSeparator = withSeparator;
      builder = new CharSeqBuilder();
    }

    public AppenderProcessor() {
      this("",false);
    }

    public AppenderProcessor(final String sep) {
      this(sep, true);
    }

    @Override
    public void process(CharSequence arg) {
      builder.append(arg);
      if (withSeparator)
        builder.append(sep);
    }

    public CharSequence sequence() {
      return builder.build();
    }

    public CharSequence trimmedSequence() {
      final CharSequence seq = builder.build();
      if (seq.length() != 0)
        return CharSeqTools.trim(seq);
      else
        return seq;
    }
  }
}
