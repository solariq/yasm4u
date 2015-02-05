package solar.mr.env;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.JSONTools;
import org.apache.log4j.Logger;
import solar.mr.*;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends RemoteMREnv {
  private static int MAX_ROW_WEIGTH = 128000000;
  private static Logger LOG = Logger.getLogger(YtMREnv.class);
  final static String mrUserHome = System.getProperty("mr.user.home","mobilesearch");
  public YtMREnv(final ProcessRunner runner, final String tag, final String master) {
    super(runner, tag, master);
  }

  @SuppressWarnings("UnusedDeclaration")
  public YtMREnv(final ProcessRunner runner, final String user, final String master,
                    final Action<CharSequence> errorsProc,
                    final Action<CharSequence> outputProc) {
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

  public int read(MRPath shard, final Processor<MRRecord> linesProcessor) {
    final int[] recordsCount = new int[]{0};

    //if (!shard.isAvailable())
    //  return 0;

    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>\"yamr");
    options.add(localPath(shard));
    executeCommand(options, new MRRoutine(shard) {
      @Override
      public void process(final MRRecord arg) {
        recordsCount[0]++;
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
    return recordsCount[0];
  }

  public void sample(MRPath table, final Processor<MRRecord> linesProcessor) {
    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(localPath(table) + "[:#100]");
    executeCommand(options, new MRRoutine(table) {
      @Override
      public void process(final MRRecord arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }


  @Override
  public MRPath[] list(final MRPath prefix) {
    final List<String> defaultOptionsEntity = defaultOptions();

    defaultOptionsEntity.add("get");
    defaultOptionsEntity.add("--format");
    defaultOptionsEntity.add("json");

    final List<String> optionEntity = new ArrayList<>();
    optionEntity.addAll(defaultOptionsEntity);
    final String path = localPath(prefix);
    final List<MRPath> result = new ArrayList<>();
    if (!prefix.isDirectory()) { // trying an easy way first
      optionEntity.add(path);
      final ConcatAction resultProcessor = new ConcatAction();
      executeCommand(optionEntity, resultProcessor, defaultErrorsProcessor, null);

      try {
        final JsonParser parser = JSONTools.parseJSON(resultProcessor.sequence());
        extractTableFromJson(prefix, result, parser);
      } catch (IOException| ParseException e) {
        LOG.warn(e);
        return new MRPath[0];
      }
    }
    else {
      final List<String> options = defaultOptions();
      options.add("list");
      final String nodePath = path.substring(0, path.length() - 1);
      options.add(nodePath);
      //final ConcatAction builder = new ConcatAction(" ");
      executeCommand(options, new Action<CharSequence>(){
        @Override
        public void invoke(CharSequence arg) {
          result.addAll(Arrays.asList(list(new MRPath(prefix.mount, prefix.path + arg, false))));
        }
      }, defaultErrorsProcessor, null);
    }
    if (result.isEmpty()) {
      updateState(prefix, new MRTableState(prefix.path,false, false, "0", 0, 0, 0, System.currentTimeMillis()));
      return new MRPath[]{prefix};
    }
    else
      return result.toArray(new MRPath[result.size()]);
  }

  private void extractTableFromJson(final MRPath prefixPath, List<MRPath> result, JsonParser parser) throws IOException, ParseException {
    final String prefix = localPath(prefixPath);
    // TODO: cache mapper
    final ObjectMapper mapper = new ObjectMapper();

    final JsonNode metaJSON = mapper.readTree(parser);
    if (metaJSON == null) {
      return;
    }

    final JsonNode typeNode = metaJSON.get("type");
    if (typeNode != null && !typeNode.isMissingNode()) {
      final String name = metaJSON.get("key").asText(); /* it's a name in Yt */
      final String path = prefixPath.isDirectory() ? prefix : prefix + "/" + name;

      if (typeNode.textValue().equals("table")) {
        final long size = metaJSON.get("uncompressed_data_size").longValue();
        boolean sorted = metaJSON.has("sorted");
        final long recordsCount = metaJSON.has("row_count") ? metaJSON.get("row_count").longValue() : 0;
        final MRTableState sh = new MRTableState(path, true, sorted, "" + size, size, recordsCount / 10, recordsCount, /*ts*/ System.currentTimeMillis());
        final MRPath localPath = findByLocalPath(path, sorted);
        result.add(localPath);
        updateState(localPath, sh);
      }
      else if (typeNode.textValue().equals("map_node")) {
        list(new MRPath(prefixPath.mount, path, false));
      }
    }
  }

  @Override
  public void copy(final MRPath[] from, MRPath to, boolean append) {
    if (!append)
      delete(to); /* Yt requires that destination shouldn't exists */
    createTable(to);

    for (final MRPath sh : from){
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
    wipeState(to);
  }

  public void write(final MRPath shard, final Reader content) {
    final String localPath = localPath(shard);
    createTable(shard);
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add(localPath);
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    updateState(shard, MRTools.updateTableShard(localPath, false, cis));
  }

  @Override
  public void append(final MRPath shard, final Reader content) {
    final String localPath = localPath(shard);
    createTable(shard);
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("\"<has_subkey=true>yamr\"");
    options.add("\"<append=true>" + localPath + "\"");
    final MRTableState cachedState = resolve(shard, true);
    if (cachedState != null) {
      MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), cachedState.recordsCount(), cachedState.keysCount(), cachedState.length());
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
      updateState(shard, MRTools.updateTableShard(localPath, false, cis));
    }
    else {
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, new InputStream() {
        @Override
        public int read() throws IOException {
          return content.read();
        }
      });
    }
  }

  private void createTable(final MRPath shard) {
    final MRTableState sh = resolve(shard, true);
    if (sh != null && sh.isAvailable())
      return;

    final List<String> options = defaultOptions();
    options.add("create");
    options.add("-r");
    options.add("table");
    options.add(localPath(shard));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    wipeState(shard);
  }

  public void delete(final MRPath table) {
    final List<String> options = defaultOptions();
    /* if (!resolve(table, false).isAvailable())
      return; */
    options.add("remove");
    options.add("-r ");
    options.add(localPath(table));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    wipeState(table);
  }

  public void sort(final MRPath table) {
    final List<String> options = defaultOptions();
    /* if (!resolve(table, false).isAvailable())
      return; */
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
    wipeState(table);
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
    MRPath[] in = builder.input();
    MRPath[] out = builder.output();

    for(final MRPath o: out) {
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

    for(final MRPath sh : in) {
      options.add("--src");
      options.add(localPath(sh));
    }

    final MRPath errorsPath = MRPath.create("/tmp/errors-" + Integer.toHexString(new FastRandom().nextInt()));
    createTable(errorsPath);
    options.add("--dst");
    options.add(localPath(errorsPath));

    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    final int[] errorsCount = new int[]{0};
    errorsCount[0] += read(errorsPath, new ErrorsTableHandler(errorsPath, errorsHandler));
    delete(errorsPath);

    return errorsCount[0] == 0;
  }

  @Override
  protected MRPath findByLocalPath(String table, boolean sorted) {
    MRPath.Mount mnt;
    String path;
    final String homePrefix = "//home/" + mrUserHome + "/";
    if (table.startsWith(homePrefix)) {
      mnt = MRPath.Mount.HOME;
      path = table.substring(homePrefix.length());
    } else if (table.startsWith("//tmp/")) {
      mnt = MRPath.Mount.TEMP;
      path = table.substring("//tmp/".length());
    } else {
      mnt = MRPath.Mount.ROOT;
      path = table;
    }

    return new MRPath(mnt, path, sorted);

  }

  @Override
  protected String localPath(MRPath shard) {
    final StringBuilder result = new StringBuilder();
    switch (shard.mount) {
      case HOME:
        result.append("//home/").append(mrUserHome).append("/");
        break;
      case TEMP:
        result.append("//tmp/");
        break;
      case ROOT:
        result.append("//");
        break;
    }
    result.append(shard.path);
    return result.toString();
  }

  @Override
  protected boolean isFat(MRPath path) {
    return false;
  }

  @Override
  public String name() {
    return "Yt://" + master + "/";
  }

  @Override
  public String toString() {
    return "Yt://" + user + "@" + master + "/";
  }

  @Override
  protected void executeCommand(List<String> options, Action<CharSequence> outputProcessor,
                                Action<CharSequence> errorsProcessor, InputStream contents) {
    if (runner instanceof SSHProcessRunner)
      super.executeCommand(options, new SshYtResponseProcessor(outputProcessor, errorsProcessor), errorsProcessor, contents);
    else
      super.executeCommand(options, outputProcessor, new LocalYtResponseProcessor(errorsProcessor), contents);
  }

  private static class ConcatAction implements Action<CharSequence> {
    private final CharSeqBuilder builder;
    private final String sep;
    private boolean withSeparator;

    private ConcatAction(final String sep, boolean withSeparator) {
      this.sep = sep;
      this.withSeparator = withSeparator;
      builder = new CharSeqBuilder();
    }

    public ConcatAction() {
      this("",false);
    }

    public ConcatAction(final String sep) {
      this(sep, true);
    }

    @Override
    public void invoke(CharSequence arg) {
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

  protected abstract static class YtResponseProcessor extends ConcatAction {
    final Action<CharSequence> processor;
    /* in SSH environment we should split outpu*/
    final static String ERROR_PROLOG = "Received an error while requesting";
    boolean ERROR = false;
    final static String CODE_TOKEN = "    code";
    final static String LOCATION_TOKEN = "    location";
    int code = 0;

    public YtResponseProcessor(final Action<CharSequence> processor) {
      this.processor = processor;
    }

    @Override
    public void invoke(CharSequence arg) {
      super.invoke(arg);
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

    public YtMRResponseProcessor(final Action<CharSequence> processor) {
      super(processor);
    }

    @Override
    public void invoke(CharSequence arg) {
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

    public LocalYtResponseProcessor(Action<CharSequence> errorProcessor) {
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
      processor.invoke(msg);
    }
  }

  private static class SshYtResponseProcessor extends YtResponseProcessor {
    final Action<CharSequence> errorProcessor;
    boolean errorMessagePrololog = false;

    public SshYtResponseProcessor(Action<CharSequence> processor, Action<CharSequence> errorProcessor) {
      super(processor);
      this.errorProcessor = errorProcessor;
    }

    @Override
    public void invoke(CharSequence arg) {
      if (!errorMessagePrololog && !CharSeqTools.startsWith(arg, ERROR_PROLOG))
        processor.invoke(arg);
      else {
        errorMessagePrololog = true;
        super.invoke(arg);
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
      errorProcessor.invoke(msg);
    }
  }
}
