package solar.mr.env;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.JSONTools;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import solar.mr.*;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Exchanger;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends RemoteMREnv {

  private static int MAX_ROW_WEIGTH = 128000000;
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
    options.add(localPath(table) + "[:#10]");
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  @Override
  public MRTableShard[] resolveAll(String[] paths) {
    final MRTableShard[] result = new MRTableShard[paths.length];
    final long time = System.currentTimeMillis();
    final Set<String> unknown = new HashSet<>();
    for(int i = 0; i < paths.length; i++) {
      final String path = paths[i];
      final MRTableShard shard = shardsCache.get("/" + path);
      if (shard != null) {
        if (time - shard.metaTS() < MRTools.FRESHNESS_TIMEOUT && shard.isAvailable()) {
          result[i] = shard;
          continue;
        }
        shardsCache.clear(path);
      }
      unknown.add(path);
    }

    for (final String u:unknown) {
      final MRTableShard[] shards = list(u);
      if (shards.length == 0) {
        continue;
      }
      assert shards.length == 1;

      int index = ArrayTools.indexOf(shards[0].path(), paths);
      if (index != -1 && shards[0].isAvailable())
        result[index] = shards[0];
    }

    for(int i = 0; i < result.length; i++) {
      if (result[i] == null)
        result[i] = new MRTableShard(paths[i], false, false, "0", 0, 0, 0, System.currentTimeMillis());
      else {
        invoke(new ShardAlter(result[i], ShardAlter.AlterType.UPDATED));
      }
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
    optionEntity.add(path + "/@");
    final AppenderProcessor getBuilder = new AppenderProcessor();
    executeCommand(optionEntity, getBuilder, defaultErrorsProcessor, null);

    final List<MRTableShard> result = new ArrayList<>();
    try {
      if (getBuilder.sequence().length() == 0)
        return new MRTableShard[0];
      JsonParser getParser = JSONTools.parseJSON(getBuilder.sequence());
      extractTableFromJson(prefix, result, getParser);
      if (!result.isEmpty()) {
        return new MRTableShard[]{result.get(0)};
      }
    } catch (IOException| ParseException e) {
      return new MRTableShard[0];
    }

    final List<String> options = defaultOptions();
    options.add("list");
    final String nodePath = path.substring(0, path.endsWith("/")?path.length() - 1: path.length());
    options.add(nodePath);
    final AppenderProcessor builder = new AppenderProcessor(" ");
    executeCommand(options, builder, defaultErrorsProcessor, null);

    final CharSequence[] listSeq = CharSeqTools.split(builder.trimmedSequence(), ' ');

    result.clear();
    for (int i = 0; i < listSeq.length; i += 1) {
      if (listSeq[i].length() == 0)
        continue;

      result.add(new WhiteboardImpl.LazyTableShard(nodePath.substring(1) /* this is in Yt form prefixed with "//" */
          + (i != listSeq.length - 1 ? "/" + listSeq[i].toString() : ""), this));
    }
    return result.toArray(new MRTableShard[result.size()]);
  }

  private void extractTableFromJson(final String prefix, List<MRTableShard> result, JsonParser parser) throws IOException, ParseException {
    final ObjectMapper mapper = new ObjectMapper();

    final JsonNode metaJSON = mapper.readTree(parser);
    final JsonNode typeNode = metaJSON.get("type");
    if (typeNode != null && !typeNode.isMissingNode() && typeNode.textValue().equals("table"))  {
      final String name = metaJSON.get("key").asText(); /* it's a name in Yt */
      final long size = metaJSON.get("uncompressed_data_size").longValue();
      boolean sorted= metaJSON.get("sorted").asBoolean();
      final long recordsCount = metaJSON.has("row_count") ? metaJSON.get("row_count").longValue() : 0;
      final String path = prefix.endsWith("/" + name)? prefix : prefix + "/" + name;
      final MRTableShard sh = new MRTableShard(path, true, sorted, "" + size, size, recordsCount/10, recordsCount, /*ts*/ System.currentTimeMillis());
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
      // is sorted enough?
      //options.add("--spec '{\"combine_chunks\"=true;\"merge_by\"=[\"key\"];\"mode\"=\"sorted\"}'");
      options.add("--spec '{\"combine_chunks\"=true;}'");
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
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    return resolve(shard.path());
  }

  public MRTableShard delete(final MRTableShard table) {
    final List<String> options = defaultOptions();
    options.add("remove");
    options.add("-r ");
    options.add(localPath(table));
    executeCommand(options, defaultOutputProcessor , defaultErrorsProcessor , null);
    final MRTableShard updatedShard = new MRTableShard(table.path(), false, false, "0", 0, 0, 0, System.currentTimeMillis());
    invoke(new ShardAlter(updatedShard, ShardAlter.AlterType.UPDATED));
    return updatedShard;
  }

  public MRTableShard sort(final MRTableShard table) {
    if (table.isSorted())
      return table;
    final List<String> options = defaultOptions();
    if (!table.isAvailable()
        && !resolve(table.path()).isAvailable())
        return table;

    final MRTableShard newShard = new MRTableShard(table.path(), true, true, table.isAvailable()? table.crc() : resolve(table.path()).crc(), table.length(), table.keysCount(), table.recordsCount(), System.currentTimeMillis());
    options.add("sort");
    options.add("--src");
    options.add(localPath(table));
    options.add("--dst");
    options.add(localPath(table));
    options.add("--sort-by key");
    options.add("--spec '{\"weight\"=5;\"sort_job_io\" = {\"table_writer\" = {\"max_row_weight\" = "
        + MAX_ROW_WEIGTH
        + "}};\"merge_job_io\" = {\"table_writer\" = {\"max_row_weight\" = "
        + MAX_ROW_WEIGTH
        + "}}}'");
    executeCommand(options, defaultOutputProcessor , defaultErrorsProcessor , null);
    invoke(new ShardAlter(newShard, ShardAlter.AlterType.CHANGED));
    return newShard;
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
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler, File jar)
  {
    final List<String> options = defaultOptions();
    switch (builder.getRoutineType()) {
      case REDUCE:
        options.add("reduce");
        options.add("--reduce-by key");
        //options.add("--spec '{\"weight\"=5}'");
        break;
      case MAP:
        options.add("map");
        break;
      default:
        throw new IllegalArgumentException("unsupported operation: " + builder.getRoutineType());
    }
    options.add("--spec '{\"weight\"=5;\"job_io\" = {\"table_writer\" = {\"max_row_weight\" = " + MAX_ROW_WEIGTH + "}}}'");
    options.add("--memory-limit 3000");
    options.add("--format");
    options.add("\"<has_subkey=true;enable_table_index=true>yamr\"");
    MRTableShard[] in = resolveAll(builder.input());
    MRTableShard[] out = resolveAll(builder.output());

    for(final MRTableShard o: out) {
      options.add("--dst");
      options.add(localPath(o));
      createTable(o); /* lazy materialization */
    }

    options.add("--local-file");
    options.add(jar.getAbsolutePath());

    options.add("'/usr/local/java8/bin/java ");
    //options.add(" -Dcom.sun.management.jmxremote.port=50042 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false");
    //options.add("-Xint -XX:+UnlockDiagnosticVMOptions -XX:+LogVMOutput -XX:LogFile=/dev/stderr ");
    options.add("-XX:-UsePerfData -Xmx2G -Xms2G -jar ");
    options.add(jar.getName()); /* please do not append to the rest of the command */
    //options.add("| sed -ne \"/^[0-9]\\*\\$/p\" -ne \"/\\t/p\" )'");
    options.add("'");

    int inCount = 0;
    for(final MRTableShard sh:in) {
      if (!resolve(sh.path()).isAvailable())
        continue;

      options.add("--src");
      options.add(localPath(sh));
      inCount ++;
    }

    if (inCount == 0) {
      for (final MRTableShard d:out) {
        createTable(d);
      }
      defaultErrorsProcessor.process("WARNING! No inputs exists map/reduce operation was skiped!");
      return true;
    }

    final String errorsShardName = "/tmp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    final MRTableShard errorsShard = createTable(new WhiteboardImpl.LazyTableShard(errorsShardName, this));
    options.add("--dst");
    options.add(localPath(errorsShard));

    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    final int[] errorsCount = new int[]{0};
    errorsCount[0] += read(errorsShard, new MRRoutine(new String[]{errorsShardName}, null, null) {
      @Override
      public void invoke(final MRRecord record) {
        CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[4]);
        /*errorsHandler.error(record.key, record.sub, new MRRecord(parts[0].toString(), parts[1].toString(), parts[2].toString(), parts[3]));
        try {
          final Exception e = (Exception)new ObjectInputStream(new ByteArrayInputStream(CharSeqTools.parseBase64(parts[1]))).readObject();
          e.printStackTrace(System.err);
        } catch (IOException e1) {
          e1.printStackTrace();
        } catch (Exception ignored) {}*/
        System.err.println(record.value);
        System.err.println(record.key + "\t" + record.sub.replace("\\n", "\n").replace("\\t", "\t"));
      }
    });
    delete(errorsShard);

    return errorsCount[0] == 0;
  }

  @Override
  protected void executeCommand(List<String> options, Processor<CharSequence> outputProcessor, Processor<CharSequence> errorsProcessor, InputStream contents) {
    if (runner instanceof SSHProcessRunner)
      super.executeCommand(options, new SshYtResponseProcessor(outputProcessor, errorsProcessor), errorsProcessor, contents);
    else
      super.executeCommand(options, outputProcessor, new LocalYtResponseProcessor(errorsProcessor), contents);
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
    final String path = "/" + shard.path();
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
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

  protected abstract static class YtResponseProcessor extends AppenderProcessor {
    final Processor<CharSequence> processor;

    /* in SSH environment we should split outpu*/
    final static String ERROR_PROLOG = "Received an error while requesting";
    boolean ERROR = false;
    final static String CODE_TOKEN = "    code";
    final static String LOCATION_TOKEN = "    location";
    int code = 0;

    public YtResponseProcessor(final Processor<CharSequence> processor) {
      this.processor = processor;
    }

    @Override
    public void process(CharSequence arg) {
      super.process(arg);
      if (CharSeqTools.startsWith(arg, CODE_TOKEN)) {
        final CharSequence[] mess = CharSeqTools.split(arg, ' ');
        code = Integer.decode(mess[16].toString());
      }
      /* Note: Yt: diagnose */
      if (code != 0 && CharSeqTools.startsWith(arg, LOCATION_TOKEN)) {
        switch (code) {
          case 500:
            warn("WARNING! Path doesn't exists");
            break;
          case 501:
            warn("WARNING! Path already exists");
            break;
          default:
            reportError();
            throw new RuntimeException("ERROR: not warning error code\n");
        }
      }
    }
    public abstract void reportError();
    public abstract void warn(final String msg);
  }

  protected abstract static class YtMRResponseProcessor extends YtResponseProcessor {
    final static int OPERATION_INITIALIZATION_POS = 14;
    boolean initialized = false;

    public YtMRResponseProcessor(final Processor<CharSequence> processor) {
      super(processor);
    }

    @Override
    public void process(CharSequence arg) {
      final CharSequence[] parts = CharSeqTools.split(arg, ' ');
      if (!initialized && CharSeqTools.equals(parts[OPERATION_INITIALIZATION_POS], "initializing")) {
        initialized = true;
        return;
      }
      return;
    }
  }

  private static class LocalYtResponseProcessor extends YtResponseProcessor {
    boolean errorMessagePrololog = false;

    public LocalYtResponseProcessor(Processor<CharSequence> errorProcessor) {
      super(errorProcessor);

    }

    @Override
    public void reportError() {
      try {
        CharSeqTools.processLines(new CharSeqReader(sequence()), processor);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void warn(final String msg) {
      processor.process(msg);
    }
  }

  private static class SshYtResponseProcessor extends YtResponseProcessor {
    final Processor<CharSequence> errorProcessor;
    boolean errorMessagePrololog = false;

    public SshYtResponseProcessor(Processor<CharSequence> processor, Processor<CharSequence> errorProcessor) {
      super(processor);
      this.errorProcessor = errorProcessor;
    }

    @Override
    public void process(CharSequence arg) {
      if (!errorMessagePrololog && !CharSeqTools.startsWith(arg, ERROR_PROLOG))
        processor.process(arg);
      else {
        errorMessagePrololog = true;
        super.process(arg);
      }
    }

    @Override
    public void reportError() {
      try {
        CharSeqTools.processLines(new CharSeqReader(sequence()), errorProcessor);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void warn(final String msg) {
      errorProcessor.process(msg);
    }
  }
}
