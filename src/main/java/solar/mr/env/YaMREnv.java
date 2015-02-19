package solar.mr.env;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.JSONTools;
import solar.mr.*;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.*;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YaMREnv extends RemoteMREnv {

  public YaMREnv(final ProcessRunner runner, final String user, final String master) {
    super(runner, user, master);
  }

  protected YaMREnv(final ProcessRunner runner, final String user, final String master,
                    final Action<CharSequence> errorsProc,
                    final Action<CharSequence> outputProc) {
    super(runner, user, master, errorsProc, outputProc);
  }

  protected List<String> defaultOptions() {
    final List<String> options = new ArrayList<>();
    { // access settings
      options.add("-subkey");

      options.add("-tableindex");

      options.add("-opt");
      options.add("user=" + user);

//      options.add("-opt");
//      options.add("stderrlevel=5");

      options.add("-server");
      options.add(master);
    }
    return options;
  }

  @Override
  public int read(MRPath shard, final Processor<MRRecord> linesProcessor) {
    final int[] recordsCount = new int[]{0};
    final List<String> options = defaultOptions();
    options.add("-read");
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
    executeCommand(options, new MRRoutine(table) {
      @Override
      public void process(final MRRecord arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    final List<MRTableState> states = new ArrayList<>();
    final List<String> options = defaultOptions();
    options.add("-list");
    options.add("-prefix");
    options.add(localPath(prefix));
    options.add("-jsonoutput");
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Action<CharSequence>() {
      @Override
      public void invoke(final CharSequence arg) {
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
            final long recordsCount = metaJSON.has("records") ? metaJSON.get("records").longValue() : 0;
            states.add(new MRTableState(name, true, "1".equals(sorted), "" + size, size, recordsCount / 10, recordsCount, System.currentTimeMillis()));
          }
          next = parser.nextToken();
        }
    } catch (IOException e) {
      throw new RuntimeException("Error parsing JSON from server: " + build, e);
    }
    /* It's discussed long time ago that because of Yamr architecture, we have to always fake existance of the tables. */
    if (states.size() == 0) {
      final MRTableState shard = new MRTableState(localPath(prefix), true, false, "0", 0, 0, 0, System.currentTimeMillis());
          updateState(prefix, shard);
      return new MRPath[]{prefix};
    }

    final List<MRPath> result = new ArrayList<>(states.size());
    for (int i = 0; i < states.size(); i++) {
      final MRTableState shard = states.get(i);
      final MRPath path = findByLocalPath(shard.path(), shard.isSorted());
      result.add(path);
      updateState(path, shard);
    }

    return result.toArray(new MRPath[states.size()]);
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    final List<String> options = defaultOptions();
    final MRTableState[] fromShards = resolveAll(from, false);
    options.add(append ? "-dstappend" : "-dst");
    options.add(localPath(to));
    options.add("-copy");
    for (int i = 0; i < from.length; i++) {
      options.add("-src");
      options.add(localPath(from[i]));
    }
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);

    final MRTableState toShard = resolve(to, false);
    if (toShard != null && ArrayTools.indexOf(null, fromShards) < 0) {
      long totalLength = append ? toShard.length() : 0;
      long recordsCount = append ? toShard.recordsCount() : 0;
      long keysCount = append ? toShard.keysCount() : 0;
      for (int i = 0; i < from.length; i++) {
        totalLength += fromShards[i].length();
        recordsCount += fromShards[i].recordsCount();
        keysCount += fromShards[i].keysCount();
      }
      final MRTableState updatedShard = new MRTableState(localPath(to), true, false, "" + totalLength, totalLength, keysCount, recordsCount, System.currentTimeMillis());
      updateState(to, updatedShard);
    }
    else wipeState(to);
  }

  @Override
  public void write(final MRPath path, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add(localPath(path));
    MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), 0, 0, 0);
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
    updateState(path, MRTools.updateTableShard(localPath(path), false, cis));
  }

  @Override
  public void append(final MRPath path, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add("-dstappend");
    options.add(localPath(path));
    MRTableState shard = resolve(path, false);
    if (shard != null) {
      MRTools.CounterInputStream cis = new MRTools.CounterInputStream(new LineNumberReader(content), shard.recordsCount(), shard.keysCount(), shard.length());
      executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, cis);
      updateState(path, MRTools.updateTableShard(localPath(path), false, cis));
    }
    else wipeState(path);
  }

  public void delete(final MRPath path) {
    final MRTableState shard = resolve(path, false);
    if (!shard.isAvailable())
      return;
    final List<String> options = defaultOptions();
    options.add("-drop");
    options.add(localPath(path));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    wipeState(path);
  }

  public void sort(final MRPath path) {
    /* final MRTableState state = resolve(path, true);
    if (state.isAvailable())
      return;*/
    final List<String> options = defaultOptions();
    options.add("-sort");
    options.add(localPath(path));
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
    wipeState(path);
  }


  private static final Set<String> FAT_DIRECTORIES = new HashSet<>(Arrays.asList(
          "user_sessions",
          "redir_log",
          "access_log",
          "reqans_log"
  ));

  protected boolean isFat(MRPath path) {
    return FAT_DIRECTORIES.contains(path.parents()[0].path);
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler, File jar) {
    final List<String> options = defaultOptions();

    final MRPath[] input = builder.input();
    final MRPath[] output = builder.output();
    int inputShardsCount = 0;
    for(int i = 0; i < input.length; i++) {
      options.add("-src");
      options.add(localPath(input[i]));
      inputShardsCount++;
    }

    for(int i = 0; i < output.length; i++) {
      options.add("-dst");
      options.add(localPath(output[i]));
    }

    final MRPath errorsPath = MRPath.create("/tmp/errors-" + Integer.toHexString(new FastRandom().nextInt()));
    options.add("-dst");
    options.add(localPath(errorsPath));

    options.add("-file");
    options.add(jar.getAbsolutePath());

    switch (builder.getRoutineType()) {
      case MAP:
        options.add("-map");
        break;
      case REDUCE:
        options.add(inputShardsCount > 1 && inputShardsCount < 10 ? "-reducews" : "-reduce");
        break;
    }

    options.add("java -XX:-UsePerfData -Xmx1G -Xms1G -jar " + jar.getName());
    final int[] errorsCount = new int[]{0};
    executeCommand(options, defaultOutputProcessor, new Action<CharSequence>() {
      String table;
      String key;
      String subkey;
      String value;
      @Override
      public void invoke(final CharSequence arg) {
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
          MRPath byLocalPath = findByLocalPath(table, false);
          // adjusting sorted attribute
          for(int i = 0; i < input.length; i++) {
            if (input[i].path.equals(byLocalPath.path)) {
              byLocalPath = findByLocalPath(table, input[i].sorted);
            }
          }
          errorsHandler.error("MR exec error", "Who knows", new MRRecord(byLocalPath, key, subkey, value));
        }
        System.err.println(arg);
      }
    }, null);
    final MRRoutine errorProcessor = new ErrorsTableHandler(errorsPath, errorsHandler);
    errorsCount[0] += read(errorsPath, errorProcessor);
    delete(errorsPath);

    if (errorsCount[0] == 0) {
      for(int i = 0; i < output.length; i++) {
        wipeState(output[i]);
      }
    }
    return errorsCount[0] == 0;
  }

  protected MRPath findByLocalPath(String table, boolean sorted) {
    MRPath.Mount mnt;
    String path;
    final String homePrefix = user + "/";
    if (table.startsWith("mobilesearch/")){
      mnt = MRPath.Mount.LOG;
      path = table.substring("mobilesearch/".length());
    }
    else if (table.startsWith(homePrefix)) {
      mnt = MRPath.Mount.HOME;
      path = table.substring(homePrefix.length());
    }
    else if (table.startsWith("temp/")) {
      mnt = MRPath.Mount.TEMP;
      path = table.substring("temp/".length());
    }
    else {
      mnt = MRPath.Mount.ROOT;
      path = table;
    }

    return new MRPath(mnt, path, sorted);
  }

  protected String localPath(MRPath shard) {
    final StringBuilder result = new StringBuilder();
    switch (shard.mount) {
      case LOG:
        result.append("mobilesearch/");
        break;
      case HOME:
        result.append(user).append("/");
        break;
      case TEMP:
        result.append("temp/");
        break;
      case ROOT:
        break;
    }
    result.append(shard.path);
    return result.toString();
  }

  @Override
  public String name() {
    return "YaMR://" + master + "/";
  }

  @Override
  public String toString() {
    return "YaMR://" + user + "@" + master + "/";
  }

}
