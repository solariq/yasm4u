package solar.mr.env;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.JSONTools;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.MRTableShard;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public class YaMREnv implements MREnv {
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
    final List<String> options = defaultOptions();
    options.add("-read");
    options.add(table.path());
    options.add("-count");
    options.add("" + 100);
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        linesProcessor.process(arg);
      }
    }, defaultErrorsProcessor, null);
  }

  public void write(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, content);
  }

  @Override
  public void append(final MRTableShard shard, final Reader content) {
    final List<String> options = defaultOptions();
    options.add("-write");
    options.add("-dstappend");
    options.add(shard.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, content);
  }

  public void delete(final MRTableShard table) {
    final List<String> options = defaultOptions();

    options.add("-drop");
    options.add(table.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
  }

  public MRTableShard sort(final MRTableShard table) {
    if (table.isSorted())
      return table;
    final List<String> options = defaultOptions();

    options.add("-sort");
    options.add(table.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
    return resolve(table.path());
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


//        {
//            "name": "user_sessions/20130901/yandex_staff",
//            "user": "userdata",
//            "chunks": 3,
//            "records": 61756,
//            "size": 280079420,
//            "full_size": 281067516,
//            "byte_size": 281067516,
//            "disk_size": 60832775,
//            "sorted": 1,
//            "write_locked": 1,
//            "mod_time": 1388675067,
//            "atime": 1388675067,
//            "creat_time": 1388675067,
//            "creat_transaction": "dbf2a388-2e7f4968-9997cded-41903667",
//            "replicas": 0,
//            "compression_algo": "zlib",
//            "block_format": "none"
//        }
  /* TODO: lazy initialization of isAvailable and crc */
  private MRTableShard shard(String shardName) {
    final List<String> options = defaultOptions();
    if (shardName.startsWith("/"))
      shardName = shardName.substring(1);
    options.add("-list");
    options.add("-prefix");
    options.add(shardName);
    options.add("-jsonoutput");
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    }, defaultErrorsProcessor, null);
    try {
      final CharSeq build = builder.build();
      JsonParser parser = JSONTools.parseJSON(build);
      ObjectMapper mapper = new ObjectMapper();
      JsonToken next = parser.nextToken();
      assert JsonToken.START_ARRAY.equals(next);
      next = parser.nextToken();
      while (!JsonToken.END_ARRAY.equals(next)) {
        final JsonNode metaJSON = mapper.readTree(parser);
        final JsonNode nameNode = metaJSON.get("name");
        if (nameNode != null && !nameNode.isMissingNode() && shardName.equals(nameNode.textValue())) {
          final String size = metaJSON.get("full_size").toString();
          final String sorted = metaJSON.get("sorted").toString();
          return new MRTableShard(shardName, this, true, "1".equals(sorted), size);
        }
        next = parser.nextToken();
      }
      return new MRTableShard(shardName, this, false, false, "");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MRTableShard resolve(final String path) {
    return shard(path);
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
    final MRTableShard errorsShard = shard(errorsShardName);
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
    options.add("java -Xmx1G -Xms1G -jar " + jarFile.getName() + " " + routineClass.getName() + " " + outputShardsCount);
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
        System.err.println(record.key + "\t" + record.sub.replace("\\n", "\n"));
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
}
