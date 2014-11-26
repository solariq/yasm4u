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
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRRoutine;
import solar.mr.MRTableShard;
import solar.mr.proc.MRState;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YaMREnv extends WeakListenerHolderImpl<MREnv.ShardAlter> implements MREnv {
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
    options.add(table.path());
    options.add("-count");
    options.add("" + 100000);
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
    options.add(prefix);
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
          result.add(new MRTableShard(name, this, true, "1".equals(sorted), "" + size, size, recordsCount/10, recordsCount, ts));
        }
        next = parser.nextToken();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error parsing JSON from server: " + build, e);
    }

    return result.toArray(new MRTableShard[result.size()]);
  }

  @Override
  public void copy(MRTableShard[] from, MRTableShard to, boolean append) {
    final List<String> options = defaultOptions();
    for(int i = 0; i < from.length; i++) {
      options.add("-src");
      options.add(from[i].path());
    }
    options.add(append ? "-dstappend" : "-dst");
    options.add(to.path());
    options.add("-copy");
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(to));
  }

  public void write(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, content);
    invoke(new ShardAlter(shard));
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add("-dstappend");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, content);
    invoke(new ShardAlter(shard));
  }

  public void delete(final MRTableShard shard) {
    if (!shard.isAvailable())
      return;
    final List<String> options = defaultOptions();
    options.add("-drop");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    invoke(new ShardAlter(shard));
  }

  public MRTableShard sort(final MRTableShard shard) {
    if (shard.isSorted())
      return shard;
    final List<String> options = defaultOptions();

    options.add("-sort");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
    invoke(new ShardAlter(shard));
    return resolve(shard.path());
  }

  private void executeCommand(final List<String> options, final Processor<CharSequence> outputProcessor,
                              final Processor<CharSequence> errorsProcessor, Reader contents) {
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
  public MRTableShard resolve(final String path) {
    return resolveAll(new String[]{path})[0];
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

  @Override
  public MRTableShard[] resolveAll(String[] paths) {
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
  public boolean execute(final Class<? extends MRRoutine> routineClass, final MRState state, final MRTableShard[] in, final MRTableShard[] out,
                         final MRErrorsHandler errorsHandler)
  {
    final List<String> options = defaultOptions();

    int inputShardsCount = 0;
    int outputShardsCount = 0;
    for(int i = 0; i < in.length; i++) {
      options.add("-src");
      options.add(in[i].path());
      inputShardsCount++;
    }
    for(int i = 0; i < out.length; i++) {
      options.add("-dst");
      options.add(out[i].path());
      outputShardsCount++;
    }
    final String errorsShardName = "temp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    options.add("-dst");
    options.add(errorsShardName);
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
    options.add("java -XX:-UsePerfData -Xmx1G -Xms1G -jar " + jarFile.getName() + " " + routineClass.getName() + " " + outputShardsCount);
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
}
