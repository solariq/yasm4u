package ru.yandex.se.yasm4u.domains.mr.env;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.EmptyIterator;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.JSONTools;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.yandex.se.yasm4u.domains.mr.*;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YtMREnv extends RemoteMREnv {
  private static int MAX_ROW_WEIGTH = 128000000;
  private static Logger LOG = Logger.getLogger(YtMREnv.class);
  final static String MR_USER_NAME = System.getProperty("user.name");

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
    options.add("<has_subkey=true>yamr");
    options.add(localPath(shard));
    final MROperation outputProcessor = new MROperation(shard) {
      @Override
      public void process(final MRRecord arg) {
        recordsCount[0]++;
        linesProcessor.process(arg);
      }
    };
    executeCommand(options, outputProcessor, defaultErrorsProcessor, null);
    outputProcessor.invoke(CharSeq.EMPTY);
    return recordsCount[0];
  }

  public void sample(MRPath table, final Processor<MRRecord> linesProcessor) {
    final List<String> options = defaultOptions();
    options.add("read");
    options.add("--format");
    options.add("<has_subkey=true>yamr");
    options.add(localPath(table) + "[:#100]");
    final MROperation outputProcessor = new MROperation(table) {
      @Override
      public void process(final MRRecord arg) {
        linesProcessor.process(arg);
      }
    };
    executeCommand(options, outputProcessor, defaultErrorsProcessor, null);
    outputProcessor.invoke(CharSeq.EMPTY);
  }

  @Override
  public void get(final MRPath prefix){
    if (prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be table");
    final List<String> attributes = getAttributesOptions();
    final List<String> optionEntity = defaultOptions();
    optionEntity.add("get");
    optionEntity.add("--format");
    optionEntity.add("json");
    optionEntity.addAll(attributes);
    optionEntity.add(localPath(prefix));

    final ConcatAction resultProcessor = new ConcatAction();
    executeCommand(optionEntity, resultProcessor, defaultErrorsProcessor, null);
    final Iterator<JsonNode> response;
    ObjectMapper mapper = new ObjectMapper();

    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,true);
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT")); // sets Server's time zone
    mapper.getSerializationConfig().with(dateFormat);
    try {
      final JsonParser parser = JSONTools.parseJSON(resultProcessor.sequence());
      JsonNode nodes = mapper.readTree(parser);
      readNode(prefix,null, mapper, nodes);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
      //return new MRPath[0];
    }
  }

  @Override
  public MRPath[] list(final MRPath prefix) {
    if (!prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be directory");
    final List<String> attributes = getAttributesOptions();
    final List<MRPath> result = new ArrayList<>();

    final List<String> optionEntity = defaultOptions();
    optionEntity.add("list");
    optionEntity.add("--format");
    optionEntity.add("json");
    optionEntity.addAll(attributes);
    final String localPath = localPath(prefix);
    optionEntity.add(localPath.substring(0, localPath.length() - 1));
    final ConcatAction resultProcessor = new ConcatAction();
    executeCommand(optionEntity, resultProcessor, defaultErrorsProcessor, null);
    final Iterator<JsonNode> response;
    ObjectMapper mapper = new ObjectMapper();

    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,true);
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT")); // sets Server's time zone
    mapper.getSerializationConfig().with(dateFormat);
    try {
      final CharSequence rawListResult = resultProcessor.sequence();
      JsonNode nodes = null;
      if (rawListResult.length() != 0) {
        final JsonParser parser = JSONTools.parseJSON(rawListResult);
        nodes = mapper.readTree(parser);
        if (!prefix.isDirectory()) {
          readNode(prefix, result, mapper, nodes);
          return result.toArray(new MRPath[result.size()]);
        }
      }
      response = nodes != null ? nodes.elements() : new EmptyIterator<JsonNode>();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
      //return new MRPath[0];
    }
    while (response.hasNext()) {
      final JsonNode node = response.next();
      readNode(prefix, result, mapper, node);
    }
    return result.toArray(new MRPath[result.size()]);
  }

  @NotNull
  private List<String> getAttributesOptions() {
    final List<String> attributes = new ArrayList<>();
    attributes.add("--attribute");
    attributes.add("type");
    attributes.add("--attribute");
    attributes.add("sorted");
    attributes.add("--attribute");
    attributes.add("row_count");
    attributes.add("--attribute");
    attributes.add("uncompressed_data_size");
    attributes.add("--attribute");
    attributes.add("modification_time");
    attributes.add("--attribute");
    attributes.add("key");
    return attributes;
  }

  private void readNode(MRPath prefix, List<MRPath> result, ObjectMapper mapper, JsonNode node) {
    YTResponse r;
    if (node == null)
      return;
    try {
      r = mapper.readValue(node.toString(), YTResponse.class);
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    final MRTableState state;
    if (r.attributes.type.equals("table")) {
      final MRPath path = prefix.isDirectory() ? MRPath.create(prefix, r.attributes.key) : prefix;
      state = new MRTableState(localPath(path),
          true, r.attributes.sorted,
          Long.toString(r.attributes.uncompressed_data_size),
          r.attributes.uncompressed_data_size, r.attributes.row_count,
              r.attributes.row_count, r.attributes.modification_time.getTime(), System.currentTimeMillis());
      if (result != null)
        result.add(path);
      updateState(new MRPath(path.mount, path.path, state.isSorted()), state);
    } else {
      if (result != null)
        result.addAll(Arrays.asList(list(MRPath.create(prefix, r.attributes.key + "/"))));
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
      options.add("--spec");
      options.add("{\"combine_chunks\"=true;}");
      options.add("--src");
      options.add(localPath(sh));
      options.add("--dst");
      options.add("<append=true>" + localPath(to));
      //options.add("--mode sorted");
      executeMapOrReduceCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    }
    wipeState(to);
  }

  public void write(final MRPath shard, final Reader content) {
    final String localPath = localPath(shard);
    createTable(shard);
    final List<String> options = defaultOptions();
    options.add("write");
    options.add("--format");
    options.add("<has_subkey=true>yamr");
    options.add(localPath);
    options.add("--table-writer");
    options.add("{\"max_row_weight\" = "
      + MAX_ROW_WEIGTH
      + "}");
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
    options.add("<has_subkey=true>yamr");
    options.add("<append=true>" + localPath);
    options.add("--table-writer");
    options.add("{\"max_row_weight\" = "
            + MAX_ROW_WEIGTH
            + "}");
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
    executeCommand(options, /* defaultOutputProcessor */ new Action<CharSequence>() {
      @Override
      public void invoke(CharSequence charSequence) {
        /* ignore */
      }
    }, defaultErrorsProcessor, null);
  }

  public void delete(final MRPath table) {
    final List<String> options = defaultOptions();
    /* if (!resolve(table, false).isAvailable())
      return; */
    options.add("remove");
    options.add("-r");
    final String path = localPath(table);
    if (table.isDirectory())
      options.add(path.substring(0, path.lastIndexOf("/")));
    else
      options.add(path);
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
    options.add("--sort-by");
    options.add("key");
    options.add("--spec");
    options.add("{\"weight\"=5;\"sort_job_io\" = {\"table_writer\" = {\"max_row_weight\" = "
        + MAX_ROW_WEIGTH
        + "}};\"merge_job_io\" = {\"table_writer\" = {\"max_row_weight\" = "
        + MAX_ROW_WEIGTH
        + "}}}");
    executeMapOrReduceCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    wipeState(table);
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler, File jar)
  {
    final List<String> options = defaultOptions();
    switch (builder.getRoutineType()) {
      case REDUCE:
        options.add("reduce");
        options.add("--reduce-by");
        options.add("key");
        //options.add("--spec '{\"weight\"=5}'");
        break;
      case MAP:
        options.add("map");
        break;
      default:
        throw new IllegalArgumentException("unsupported operation: " + builder.getRoutineType());
    }
    options.add("--spec");
    options.add("{weight=5;job_io = {table_writer = {max_row_weight = " + MAX_ROW_WEIGTH + "}}}");
    options.add("--memory-limit");
    options.add("3000");
    options.add("--format");
    options.add("<has_subkey=true;enable_table_index=true>yamr");
    MRPath[] in = builder.input();
    MRPath[] out = builder.output();


    options.add("--local-file");
    options.add(jar.getAbsolutePath());

    options.add("/usr/local/java7/bin/java "
      + (Boolean.getBoolean("yasm4u.enableJMX")? "-Dcom.sun.management.jmxremote " : "")
      + (Boolean.getBoolean("yasm4u.loggc")? "-Xloggc:/dev/stderr -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -verbose:gc":"")
      + " -XX:-UsePerfData -XX:+PerfDisableSharedMem -Xmx2G -Xms2G -jar " + jar.getName());
    int inputCount = 0;
    for(final MRPath sh : in) {
      if (!resolve(sh, false).isAvailable()) {
        defaultErrorsProcessor.invoke("WARNING! " + sh + " isn't available. ");
        continue;
      }
      options.add("--src");
      options.add(localPath(sh));
      inputCount++;
    }

    /* Otherwise Yt fails with wrong command syntax. */
    if (inputCount == 0) {
      defaultErrorsProcessor.invoke("WARNING!: operation: " + builder.getRoutineType() + " " + builder.toString() + " is skipped");
      return true;
    }

    for(final MRPath o: out) {
      options.add("--dst");
      options.add(localPath(o));
      createTable(o); /* lazy materialization */
    }

    final MRPath errorsPath = MRPath.create("/tmp/errors-" + Integer.toHexString(new FastRandom().nextInt()));
    createTable(errorsPath);
    options.add("--dst");
    options.add(localPath(errorsPath));

    executeMapOrReduceCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    final MROperation errorProcessor = new ErrorsTableHandler(errorsPath, errorsHandler);
    final int errorsCount = read(errorsPath, errorProcessor);
    errorProcessor.invoke(CharSeq.EMPTY);
    if (errorsCount == 0)
      delete(errorsPath);

    return errorsCount == 0;
  }

  @Override
  protected MRPath findByLocalPath(String table, boolean sorted) {
    MRPath.Mount mnt;
    String path;
    // see about homes https://st.yandex-team.ru/YTADMIN-1575
    final String homePrefix = "//home/mobilesearch/personal_homes/" + MR_USER_NAME + "/";
    if (table.startsWith(homePrefix)) {
      mnt = MRPath.Mount.HOME;
      path = table.substring(homePrefix.length());
    }
    else if (table.startsWith("//home/mobilesearch/")) {
      mnt = MRPath.Mount.LOG;
      path = table.substring("//home/mobilesearch/".length());
    }
    else if (table.startsWith("//tmp/")) {
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
      case LOG:
        result.append("//home/mobilesearch/");
        break;
      case HOME: //https://st.yandex-team.ru/YTADMIN-1575
        result.append("//home/mobilesearch/personal_homes/").append(MR_USER_NAME).append("/");
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
    final String localPath = localPath(path);
    if ("//home/mobilesearch/logprocessing.daily/paradiso/".equals(localPath))
      return true;
    return false;
  }

  @Override
  public String name() {
    return "Yt//" + master + "/";
  }

  @Override
  public String toString() {
    return "Yt//" + user + "@" + master + "/";
  }

  private void executeMapOrReduceCommand(final List<String> options, final Action<CharSequence> outputProcessor, final Action<CharSequence> errorsProcessor, final InputStream contents) {
    final YtMRResponseProcessor processor;
    if (runner instanceof SSHProcessRunner)
      super.executeCommand(options, (processor = new SshMRYtResponseProcessor(outputProcessor, errorsProcessor)), errorsProcessor, contents);
    else
      super.executeCommand(options, outputProcessor, (processor = new LocalMRYtResponseProcessor(errorsProcessor)), contents);

    if (!processor.isOk())
      throw new RuntimeException("M/R failed");
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

  public static class YTResponse {
    final YTTableDescriptor attributes;
    final String value;
    @JsonCreator
    public YTResponse(@JsonProperty("$attributes") final YTTableDescriptor attributes,
                      @JsonProperty("$value") final String value) {
      this.attributes = attributes;
      this.value = value;
    }
  }

  public static class YTTableDescriptor {
    public final String key;
    public final String type;
    public final boolean sorted;
    public final long row_count;
    public final long uncompressed_data_size;
    public final Date modification_time;

    @JsonCreator
    public  YTTableDescriptor(@JsonProperty("key") final String key,
                           @JsonProperty("type") final String type,
                           @JsonProperty("sorted") boolean sorted,
                           @JsonProperty("row_count") long row_count,
                           @JsonProperty("uncompressed_data_size") long uncompressed_data_size,
                           @JsonProperty("modification_time") Date modificationTime)
    {
      this.key = key;
      this.type = type;
      this.sorted = sorted;
      this.row_count = row_count;
      this.uncompressed_data_size = uncompressed_data_size;
      this.modification_time = modificationTime;
    }
  }

  protected abstract static class YtResponseProcessor implements Action<CharSequence> {
    final Action<CharSequence> processor;

    public YtResponseProcessor(final Action<CharSequence> processor) {
      this.processor = processor;
    }

    @Override
    public void invoke(CharSequence arg) {
      /* table guid 5ad-2a7267-3f10191-8b0038b3 */
      if (arg.length() > 18
          && arg.charAt(3) == '-'
          && (arg.charAt(10) == '-' || arg.charAt(9) == '-')
          && (arg.charAt(18) == '-' || arg.charAt(17) == '-'))
        return;
      try {
        final JsonParser parser = JSONTools.parseJSON(arg);
        if (parser == null) {
          processor.invoke(arg);
          return;
        }
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode metaJSON = mapper.readTree(parser);
        /* TODO: more protective programming */
        int code = 0;
        if (metaJSON == null || !metaJSON.has("message")) {
          processor.invoke(arg);
          return;
        }
        JsonNode errors = metaJSON.get("inner_errors").get(0);
        do {
          code = errors.get("code").asInt();
          errors = errors.elements().next().get(0);
          if (errors == null)
            break;
        } while (errors.size() != 0);
        errorCodeResolver(arg, metaJSON, code);
      } catch (JsonParseException e) {
        if (System.getProperty("user.name").equals("minamoto")) {
          reportError("Msg: " + arg.toString() + " appears here by mistake!!!!");
          reportError(e.getMessage());
        }
        processor.invoke(arg);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private void errorCodeResolver(CharSequence arg, JsonNode metaJSON, int code) {
      JsonNode errors;
      switch (code) {
        case 500:
          warn("WARNING! doesn't exists");
          break;
        case 501:
          warn("WARNING! already exists");
          break;
        case 1:
          errors = metaJSON.get("inner_errors").get(0);
          errorCodeResolver(arg, errors, errors.get("code").asInt());
          break;
        default: {
          reportError(arg);
          throw new RuntimeException("Yt exception");
        }
      }
    }

    public abstract void reportError(final CharSequence msg);
    public abstract void warn(final String msg);
  }

  protected abstract static class YtMRResponseProcessor extends YtResponseProcessor {
    enum OperationStatus {
      NONE,
      INITIALIZING,
      PREPARING,
      COMPLETING,
      FAILED,
      COMPETED,
      PRINT_HINT
    }
    private OperationStatus status = OperationStatus.NONE;
    private CharSequence guid = null;
    private final static String TOK_OPERATION = "operation";
    private final static String TOK_OP_INITIALIZING = "initializing";
    private final static String TOK_OP_COMPLETING = "completing";
    private final static String TOK_OP_COMPLETED = "completed";
    private final static String TOK_OP_PREPARING = "preparing";
    private final static String TOK_OP_FAILED = "failed";

    private final static String TOK_HINT = "INFO";

    public YtMRResponseProcessor(final Action<CharSequence> processor) {
      super(processor);
    }

    public boolean isOk() {
      return (status != OperationStatus.FAILED);
    }

    private CharSequence eatDate(final CharSequence arg) {
      if (CharSeqTools.isNumeric(arg.subSequence(0,3)) /* year */
          && arg.charAt(4) == '-'
          && CharSeqTools.isNumeric(arg.subSequence(5, 6)) /* month */
          && arg.charAt(7) == '-'
          && CharSeqTools.isNumeric(arg.subSequence(8,9)) /* day */)
        return arg.subSequence(10,arg.length());
      else {
        reportError(arg);
        return arg.subSequence(10,arg.length());
        //throw new RuntimeException("Expected date");
      }
    }

    private CharSequence eatTime(final CharSequence arg, char separator) {
      if (CharSeqTools.isNumeric(arg.subSequence(0,1)) /* hours */
          && arg.charAt(2) == ':'
          && CharSeqTools.isNumeric(arg.subSequence(3,4)) /* minutes */
          && arg.charAt(5) == ':'
          && CharSeqTools.isNumeric(arg.subSequence(6,7))
          && arg.charAt(8) == separator
          && CharSeqTools.isNumeric(arg.subSequence(9,11)))
        return arg.subSequence(12, arg.length());
      else {
        reportError(arg);
        //throw new RuntimeException("Expected time hh:MM:ss " + separator + " zzz");
        return arg.subSequence(12, arg.length());
      }
    }

    private CharSequence eatPeriod(final CharSequence arg) {
      int index = 3;
      if (arg.charAt(0) == '(') {
        while (CharSeqTools.isNumeric(arg.subSequence(2, index))) {
          index++;
        }
        if (CharSeqTools.equals(arg.subSequence(index, index + 4),"min)"))
          return arg.subSequence(index + 5, arg.length());
      }
      reportError(arg);
      return arg.subSequence(index + 5, arg.length());
      //throw new RuntimeException("Expected period \"( xx min)\"");
    }

    private CharSequence eatToken(final CharSequence arg, final String token) {
      if (!CharSeqTools.startsWith(arg, token)
          || CharSeqTools.isAlpha(arg.subSequence(token.length(), token.length() + 1))) {
        reportError(arg);
        //throw new RuntimeException("expected token: " + token);
        return arg.subSequence(token.length() + 1, arg.length());
      }
      return arg.subSequence(token.length() + 1, arg.length());
    }

    private CharSequence initGuid(final CharSequence arg) {
      CharSequence guid = arg.subSequence(0, CharSeqTools.indexOf(arg, " "));
      if (this.guid != null && !CharSeqTools.equals(guid, this.guid)) {
        reportError(arg);
        //throw new RuntimeException("something strange with guid");
        return arg.subSequence(guid.length(), arg.length());
      }
      else if (this.guid == null){
        this.guid = guid;
      }
      else if (!this.guid.equals(guid)) {
        warn("NOTICE! Guid has changed " + this.guid + " -> " + guid);
        this.guid = guid;
      }
      return arg.subSequence(guid.length(), arg.length());
    }

    private void checkOperationStatus(final CharSequence arg) {
      if (CharSeqTools.equals(arg, TOK_OP_INITIALIZING) && status == OperationStatus.NONE) {
        status = OperationStatus.INITIALIZING;
        return;
      }
      if (CharSeqTools.equals(arg, TOK_OP_COMPLETED)
          && (status == OperationStatus.INITIALIZING
          || status == OperationStatus.PREPARING
          || status == OperationStatus.PRINT_HINT
          || status == OperationStatus.NONE /* Ultra fast operation usually with empty inputs */
          || status == OperationStatus.COMPLETING)){
        status = OperationStatus.COMPETED;
        return;
      }
      if (CharSeqTools.equals(arg, TOK_OP_PREPARING)
          && (status == OperationStatus.INITIALIZING)) {
        status = OperationStatus.PREPARING;
        return;
      }
      if (CharSeqTools.equals(arg, TOK_OP_COMPLETING)
          && (status == OperationStatus.NONE
          || status == OperationStatus.INITIALIZING)) {
        status = OperationStatus.COMPLETING;
        return;
      }
      if (CharSeqTools.equals(arg, TOK_OP_FAILED)) {
        status = OperationStatus.FAILED;
        reportError("FAILED");
        return;
        //throw new RuntimeException("Operation failed");

      }
      reportError("current status: " + status);
      return;
      //throw new RuntimeException("Unknown status: " + arg);
    }

    private CharSequence eatWord(final CharSequence arg, final String word){
      if (CharSeqTools.startsWith(arg, word)
          && (arg.charAt(word.length()) == ' '
      || arg.charAt(word.length()) == '\t')) {
        return arg.subSequence(word.length(), arg.length());
      }
      else throw new RuntimeException("Wrong word! '" + arg + "'");
    }

    private void hint(final CharSequence arg) {
      processor.invoke(CharSeqTools.trim(eatToken(arg, TOK_HINT)));
      status = OperationStatus.PRINT_HINT;
    }

    @Override
    public void invoke(CharSequence arg) {
      if (arg == CharSeq.EMPTY) {
        processor.invoke(arg);
      }
      warn("DEBUG:" + arg);
      if (arg.length() == 0)
        return;
      switch (status) {
        case NONE:
        case INITIALIZING:
          final CharSequence raw0 = CharSeqTools.trim(eatPeriod(CharSeqTools.trim(eatTime(CharSeqTools.trim(eatDate(arg)), '.'))));
          final CharSequence raw1 = CharSeqTools.trim(eatToken(raw0, TOK_OPERATION));
          final CharSequence raw2 = CharSeqTools.trim(initGuid(raw1));
          /* we don't need the rest of the mess at runtime
           * in some cases Yt drops : before running=... failed=...
           */
          if (raw2.charAt(0) == ':' || CharSeqTools.startsWith(CharSeqTools.trim(raw2), "running="))
            return;
          checkOperationStatus(raw2);
          break;
        case COMPETED:
          hint(CharSeqTools.trim(eatTime(CharSeqTools.trim(eatDate(arg)), ',')));
          break;
        case PRINT_HINT:
          processor.invoke(arg);
          status = OperationStatus.COMPETED;
          break;
        case FAILED:
          final CharSequence result = eatWord(CharSeqTools.trim(initGuid(CharSeqTools.trim(
              eatWord(CharSeqTools.trim(arg), "Operation")))), "failed. Result:");
          final YtResponseProcessor codeResolver = new YtResponseProcessor(new Action<CharSequence>() {
            @Override
            public void invoke(final CharSequence charSequence) {
              YtMRResponseProcessor.this.reportError(charSequence);
            }
          }){
            @Override
            public void reportError(CharSequence msg) {
              status = OperationStatus.FAILED;
            }

            @Override
            public void warn(String msg) {
              YtMRResponseProcessor.this.warn(msg);
              status = OperationStatus.COMPETED;
            }
          };
          try {
            CharSeqTools.processLines(new CharSeqReader(result), codeResolver);
          } catch (IOException e) {
            status = OperationStatus.FAILED;
            throw new RuntimeException(e);
          }
          break;
        default:
          reportError(arg);
          //throw new RuntimeException("Please add case!!!");
      }
      /* here should be hint processing */
    }
  }

  private static class LocalYtResponseProcessor extends YtResponseProcessor {
    public LocalYtResponseProcessor(Action<CharSequence> errorProcessor) {
      super(errorProcessor);
    }

    @Override
    public void reportError(final CharSequence msg) {
      processor.invoke(msg);
    }

    @Override
    public void warn(final String msg) {
      processor.invoke(msg);
    }
  }

  private static class SshYtResponseProcessor extends YtResponseProcessor {
    final Action<CharSequence> errorProcessor;
    public SshYtResponseProcessor(Action<CharSequence> processor, Action<CharSequence> errorProcessor) {
      super(processor);
      this.errorProcessor = errorProcessor;
    }

    @Override
    public void invoke(CharSequence arg) {
      if (CharSeqTools.indexOf(arg, "\t") == -1)
        super.invoke(CharSeqTools.trim(arg));
      else
        processor.invoke(arg);
    }

    @Override
    public void reportError(final CharSequence errorMsg) {
      if (System.getProperty("user.name").equals("minamoto"))
        errorProcessor.invoke(errorMsg);
    }

    @Override
    public void warn(final String msg) {
      if (System.getProperty("user.name").equals("minamoto"))
        errorProcessor.invoke(msg);
    }
  }

  protected static class LocalMRYtResponseProcessor extends YtMRResponseProcessor{
    public LocalMRYtResponseProcessor(Action<CharSequence> processor) {
      super(processor);
    }

    @Override
    public void reportError(CharSequence msg) {
      processor.invoke(msg);
    }

    @Override
    public void warn(String msg) {
      processor.invoke(msg);
    }
  }

  protected static class SshMRYtResponseProcessor extends YtMRResponseProcessor{
    final Action<CharSequence> errorProcessor;
    public SshMRYtResponseProcessor(final Action<CharSequence> processor, final Action<CharSequence> errorProcessor) {
      super(processor);
      this.errorProcessor = errorProcessor;
    }

    @Override
    public void reportError(CharSequence msg) {
      if (System.getProperty("user.name").equals("minamoto")) {
        errorProcessor.invoke(msg);
      }
    }

    @Override
    public void warn(String msg) {
      if (System.getProperty("user.name").equals("minamoto")) {
        errorProcessor.invoke(msg);
      }
    }
  }
}
