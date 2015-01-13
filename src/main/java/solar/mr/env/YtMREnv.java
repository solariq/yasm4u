package solar.mr.env;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.JSONTools;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import solar.mr.*;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRRecord;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends RemoteMREnv {
  public YtMREnv(final ProcessRunner runner, final String tag, final String master) {
    super(runner, tag, master);
  }

  @SuppressWarnings("UnusedDeclaration")
  public YtMREnv(final ProcessRunner runner, final String user, final String master,
                    final Processor<CharSequence> errorsProc,
                    final Processor<CharSequence> outputProc) {
    super(runner, user, master, errorsProc, outputProc);
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
    executeCommand(options, new YtResponseProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        recordsCount[0]++;
        linesProcessor.process(arg);
      }
    }), defaultErrorsProcessor, null);
    return recordsCount[0];
  }

  public void sample(MRTableShard table, final Processor<CharSequence> linesProcessor) {
    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(localPath(table) + "[:#100]");
    executeCommand(options, new YtResponseProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }), defaultErrorsProcessor, null);
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths) {
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

    for (final String u:unknown) {
      final MRTableShard[] shards = list(u);
      assert shards.length <= 1;
      if (shards.length != 1)
        continue;

      int index = ArrayTools.indexOf(shards[0].path(), paths);
      if (shards[0].isAvailable())
        result[index] = shards[0];
    }

    for(int i = 0; i < result.length; i++) {
      if (result[i] == null)
        result[i] = new MRTableShard(paths[i], false, false, "0", 0, 0, 0, System.currentTimeMillis());
      else {
        shardsCache.put(paths[i], result[i]);
        invoke(new ShardAlter(result[i], ShardAlter.AlterType.UPDATED));
      }
    }
    return result;
  }

  @Override
  public MRTableShard[] list(String prefix) {
    final List<String> defaultOptionsEntity = defaultOptions();
    final MRTableShard sh = shardsCache.get(prefix);
    final long time = System.currentTimeMillis();
    if (sh != null && time - sh.metaTS() < MRTools.FRESHNESS_TIMEOUT)
      return new  MRTableShard[]{sh};

    defaultOptionsEntity.add("get");
    defaultOptionsEntity.add("--format");
    defaultOptionsEntity.add("json");

    final List<String> optionEntity = new ArrayList<>();
    optionEntity.addAll(defaultOptionsEntity);
    final String path = localPath(new WhiteboardImpl.LazyTableShard(prefix, this));
    optionEntity.add(path + (prefix.endsWith("/")? "" : "/") + "@");
    final AppenderProcessor getBuilder = new AppenderProcessor();
    executeCommand(optionEntity, new YtResponseProcessor(getBuilder), defaultErrorsProcessor, null);

    final List<MRTableShard> result = new ArrayList<>();
    try {
      if (getBuilder.sequence().length() == 0)
        return new MRTableShard[0];
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
      final String path = prefix.endsWith("/" + name)? prefix : prefix + "/" + name;
      final MRTableShard sh = new MRTableShard(path, true, sorted, "" + size, size, recordsCount/10, recordsCount, ts);
      result.add(sh);
      invoke(new ShardAlter(sh, ShardAlter.AlterType.UPDATED));
    }
  }

  @Override
  public MRTableShard copy(final MRTableShard[] from, MRTableShard to, boolean append) {
    for (final  MRTableShard d:from) {
      if (!resolve(d.path()).isAvailable())
        createTable(d);
    }
    if (!append) {
      delete(to); /* Yt requires that destination shouldn't exists */
    }
    createTable(to);

    for (final MRTableShard sh:from){
      final List<String> options = defaultOptions();
      options.add("merge");
      options.add("--src");
      options.add(localPath(sh));
      options.add("--dst");
      options.add("\"<append=true>\"" + localPath(to));
      //options.add("--mode sorted");
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
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
    if (sh != null && resolve(sh.path()).isAvailable())
      return sh;

    final List<String> options = defaultOptions();
    options.add("create");
    options.add("-r");
    options.add("table");
    options.add(localPath(shard));
    executeCommand(options, new YtResponseProcessor(defaultOutputProcessor), defaultErrorsProcessor, null);
    return resolve(shard.path());
  }

  public MRTableShard delete(final MRTableShard table) {
    final List<String> options = defaultOptions();

    options.add("remove");
    options.add("-r");
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
  public MRTableShard resolve(final String path) {
    return resolveAll(new String[]{path})[0];
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler)
  {
    final List<String> options = defaultOptions();
    switch (builder.getRoutineType()) {
      case REDUCE:
        options.add("map-reduce");
        options.add("--reduce-by key");
        options.add("--sort-by key");
        options.add("--reduce-memory-limit 2000");
        options.add("--reducer");

        break;
      case MAP:
        options.add("map");
        //noinspection fallthrough
      default:
        options.add("--memory-limit 2000");
        break;
    }

    options.add("--format");
    options.add("\"<has_subkey=true;enable_table_index=true>yamr\"");
    MRTableShard[] in = resolveAll(builder.input());
    MRTableShard[] out = resolveAll(builder.output());

//    final MRTableShard[] realOut = new MRTableShard[out.length];
    for(int i = 0; i < out.length; i++) {
      options.add("--dst");
      options.add(localPath(out[i]));
//      realOut[i] =
      createTable(out[i]); /* lazy materialization */
    }

    final File jarFile = builder.buildJar(this, errorsHandler);


    options.add("'/usr/local/java8/bin/java -XX:-UsePerfData -Xmx1G -Xms1G -jar ");
    options.add(jarFile.getName()); /* please do not append to the rest of the command */

    int inCount = 0;
    for(final MRTableShard sh:in) {
      options.add("--src");
      options.add(localPath(sh));
      inCount ++;
    }
    if (inCount == 0) {
      for (final MRTableShard d:out) {
        createTable(d);
      }
      return true;
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
    errorsCount[0] += read(errorsShard, new MRRoutine(new String[]{errorsShardName}, null, null) {
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

  private class YtResponseProcessor implements Processor<CharSequence> {
    final Processor<CharSequence> processor;
    boolean skip = false;
    public YtResponseProcessor(final Processor<CharSequence> processor) {
      this.processor = processor;
    }
    @Override
    public void process(CharSequence arg) {
      /* TODO: enhance error/warnings processing */
      if (!skip && CharSeqTools.startsWith(arg, "Response to request"))
        skip = true;

      if (!skip)
        processor.process(arg);
    }
  }
}
