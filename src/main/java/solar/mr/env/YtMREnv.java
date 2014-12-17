package solar.mr.env;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


import com.fasterxml.jackson.core.JsonParser;
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
import org.apache.commons.lang3.NotImplementedException;
import solar.mr.*;
import solar.mr.proc.State;
import solar.mr.MRTableShard;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends BaseEnv implements ProfilableMREnv {

  private final Random rng = new FastRandom();
  public YtMREnv(final ProcessRunner runner, final String tag, final String master) {
    super(runner, tag, master);
  }

  protected YtMREnv(final ProcessRunner runner, final String user, final String master,
                    final Processor<CharSequence> errorsProc,
                    final Processor<CharSequence> outputProc,
                    ClosureJarBuilder jarBuilder) {
    super(runner, user, master, errorsProc, outputProc, jarBuilder);
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

    //if (!shard.isAvailable())
    //  return 0;

    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>\"yamr");
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
    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(localPath(table) + "[:#100]");
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  private static final Set<String> FAT_DIRECTORIES = new HashSet<>(Arrays.asList(
      "/userdata/user_sessions"
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
  public boolean execute(Class<? extends MRRoutine> exec, State state, MRTableShard[] in, MRTableShard[] out, MRErrorsHandler errorsHandler) {
    return execute(exec, state, in, out, errorsHandler, EMPTY_PROFILER);
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths, final Profiler profiler) {
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
      final MRTableShard[] list = list(prefix);
      for(int i = 0; i < list.length; i++) {
        final MRTableShard shard = list[i];
        final int index = ArrayTools.indexOf(localPath(shard), paths);
        if (index >= 0)
          result[index] = shard;
      }
    }
    for(int i = 0; i < result.length; i++) {
      if (result[i] == null)
        result[i] = new MRTableShard(paths[i], this, false, false, "0", 0, 0, 0, System.currentTimeMillis());
      shardsCache.put(paths[i], result[i]);
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
    final String path = localPath(new WhiteboardImpl.LazyTableShard(prefix, this));
    optionEntity.add(path + (prefix.endsWith("/")? "" : "/") + "@");
    final AppenderProcessor getBuilder = new AppenderProcessor();
    executeCommand(optionEntity, getBuilder, defaultErrorsProcessor, null);

    final List<MRTableShard> result = new ArrayList<>();
    try {
      JsonParser getParser = JSONTools.parseJSON(getBuilder.sequence());
      extractTableFromJson(prefix, result, getParser);
      if (!result.isEmpty()) {
        shardsCache.put(prefix, result.get(0));
        return new MRTableShard[]{result.get(0)};
      }
    } catch (IOException| ParseException e) {
      return new MRTableShard[0];
    }

    final List<String> options = defaultOptions();
    options.add("list");
    final String nodePath = path.substring(0, path.length() - 1);
    options.add(nodePath);
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
      result.add(new WhiteboardImpl.LazyTableShard(nodePath.substring(1) /* this is in Yt form prefixed with "//" */
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
  public MRTableShard copy(MRTableShard[] from, MRTableShard to, boolean append) {
    int startIndex = 0;
    if (!append) {
      delete(to); /* Yt requires that destination shouldn't exists */
      final List<String> options = defaultOptions();
      options.add("copy");
      options.add(localPath(from[0]));
      options.add(localPath(to));
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
      startIndex = 1;
    }

    if (from.length > 1) {
      for (int i = startIndex; i < from.length; ++i) {
        final List<String> options = defaultOptions();
        options.add("merge");
        options.add("--src");
        options.add(localPath(from[i]));
        options.add("--dst");
        options.add("\"<append=true>\"" + localPath(to));
        //options.add("--mode sorted");
        executeCommand(options,defaultOutputProcessor, defaultErrorsProcessor, null);
      }
    }
    invoke(new ShardAlter(to, ShardAlter.AlterType.CHANGED));
    return to;
  }

  public MRTableShard write(final MRTableShard shard, final Reader content) {
    createTable(shard);
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(localPath(shard));
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    final MRTableShard updatedShard = MRTools.updateTableShard(shard, false, cis);
    invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    return updatedShard;
  }

  @Override
  public MRTableShard append(final MRTableShard shard, final Reader content) {
    createTable(shard);
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add("\"<append=true>" + localPath(shard) + "\"");
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), shard.recordsCount(), shard.keysCount(), shard.length());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    final MRTableShard updatedShard = MRTools.updateTableShard(shard, false, cis);
    invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    return updatedShard;
  }

  private MRTableShard createTable(final MRTableShard shard) {
    final MRTableShard sh = shardsCache.get(shard.path());
    if (sh != null)
      return sh;

    final List<String> options = defaultOptions();
    options.add("create");
    options.add("-r");
    options.add("table");
    options.add(localPath(shard));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    return resolve(shard.path());
  }

  public MRTableShard delete(final MRTableShard table) {
    final List<String> options = defaultOptions();

    options.add("remove");
    options.add(localPath(table));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(table));
    shardsCache.clear(table.path());
    return table;
  }

  public MRTableShard sort(final MRTableShard table) {
    if (table.isSorted())
      return table;
    final List<String> options = defaultOptions();
    options.add("sort");
    options.add("--src");
    options.add(localPath(table));
    options.add("--dst");
    options.add(localPath(table));
    options.add("--sort_by key");
    invoke(new ShardAlter(table, ShardAlter.AlterType.CHANGED));
    return table;
  }

  @Override
  public String getTmp() {
    return "/tmp/";
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
  public boolean execute(final Class<? extends MRRoutine> routineClass, final State state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler, Profiler profiler)
  {
    final List<String> options = defaultOptions();
    if (MRMap.class.isAssignableFrom(routineClass))
      options.add("map");
    else if (MRReduce.class.isAssignableFrom(routineClass)) {
      //options.add(inputShardsCount > 1 && inputShardsCount < 10 ? "-reducews" : "-reduce");
      options.add("map-reduce");
      options.add("--reduce-by key");
      options.add("--sort-by key");
    } else
      throw new RuntimeException("Unknown MR routine type");

    options.add("--format");
    options.add("\"<has_subkey=true;enable_table_index=true>yamr\"");

    final MRTableShard[] realOut = new MRTableShard[out.length];
    for(int i = 0; i < out.length; i++) {
      options.add("--dst");
      options.add(localPath(out[i]));
      realOut[i] = createTable(out[i]); /* lazy materialization */
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
      if (MRReduce.class.isAssignableFrom(routineClass))
        options.add("--reduce-local-file");
      else
        options.add("--local-file");
      options.add(jarFile.getAbsolutePath());
    }

    if (MRReduce.class.isAssignableFrom(routineClass)) {
      options.add("--reduce-memory-limit 2000");
      options.add("--reducer");
    }
    else {
      options.add("--memory-limit 2000");
    }

    options.add("'/usr/local/java8/bin/java -XX:-UsePerfData -Xmx1G -Xms1G -jar ");
    options.add(jarFile.getName()); /* please do not append to the rest of the command */
    options.add(" " + routineClass.getName() + " " + out.length + " " + profiler.isEnabled() + "'");
    for(int i = 0; i < in.length; i++) {
      options.add("--src");
      options.add(localPath(in[i]));
    }

    final String errorsShardName = "/tmp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    final MRTableShard errorsShard = createTable(new WhiteboardImpl.LazyTableShard(errorsShardName, this));
    options.add("--dst");
    options.add(localPath(errorsShard));

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
    return "YaMR://" + user + "@" + master + "/";
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

  private String upload(final String from, final String to) {

    final String toPath = "//tmp/" + user + "/state/files";
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

  private String localPath(MRTableShard shard) {
    return "/" + shard.path();
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
