package solar.mr.env;

import java.io.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Computable;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.JSONTools;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.tables.DailyMRTable;
import solar.mr.tables.FixedMRTable;
import solar.mr.tables.MRTableShard;
import solar.mr.tables.MRTableShardBase;

/**
 * User: solar
 * Date: 19.09.14
 * Time: 17:08
 */
public abstract class YaMREnvBase implements MREnv {
  private final String user;

  private final String master;
  protected Processor<CharSequence> defaultErrorsProcessor;
  protected Processor<CharSequence> defaultOutputProcessor;
  private final ClosureJarBuilder jarBuilder;

  protected YaMREnvBase(final String user, final String master) {
    this(user, master,
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

  protected YaMREnvBase(final String user, final String master,
                        final Processor<CharSequence> errorsProc,
                        final Processor<CharSequence> outputProc,
                        ClosureJarBuilder jarBuilder) {
    this.user = user;
    this.master = master;
    this.defaultErrorsProcessor = errorsProc;
    this.defaultOutputProcessor = outputProc;
    this.jarBuilder = jarBuilder;
  }

  protected abstract Process runYaMRProcess(List<String> mrOptions);

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

  public void sort(final MRTableShard table) {
    final List<String> options = defaultOptions();

    options.add("-sort");
    options.add(table.path());
    executeCommand(options, defaultOutputProcessor, defaultErrorsProcessor, null);
    options.remove(options.size() - 1);
  }

  private void executeCommand(final List<String> options, final Processor<CharSequence> outputProcessor,
                              final Processor<CharSequence> errorsProcessor, final Reader input) {
    try {
      final Process exec = runYaMRProcess(options);
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
      if (input != null)
        StreamTools.transferData(input, new OutputStreamWriter(exec.getOutputStream(), StreamTools.UTF));
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
  public MRTableShard[] shards(final MRTable table) {
    MRTableShard[] result;
    if (table instanceof DailyMRTable) {
      final DailyMRTable daily = (DailyMRTable) table;
      result = new MRTableShard[daily.length()];
      for (int i = 0; i < daily.length(); i++) {
        result[i] = shard(daily.shardName(i), table);
      }
    }
    else if (table instanceof FixedMRTable) {
      result = new MRTableShard[]{shard(table.name(), table)};
    }
    else throw new IllegalArgumentException("Unsupported table type");
    return result;
  }


  private MRTableShard shard(final String shardName, final MRTable owner) {
    final List<String> options = defaultOptions();
    options.add("-list");
    options.add("-prefix " + shardName);
    options.add("-jsonoutput");
    final CharSeqBuilder builder = new CharSeqBuilder();
    executeCommand(options, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    }, defaultErrorsProcessor, null);
    try {
      JsonParser parser = JSONTools.parseJSON(builder.build());
      ObjectMapper mapper = new ObjectMapper();
      assert JsonToken.START_ARRAY.equals(parser.nextToken());
      JsonToken next = parser.nextToken();
      while (!JsonToken.END_ARRAY.equals(next)) {
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
        final JsonNode metaJSON = mapper.readTree(parser);
        final JsonNode nameNode = metaJSON.path("name");
        if (!nameNode.isMissingNode() && shardName.equals(nameNode.toString())) {
          final String size = metaJSON.path("full_size").toString();
          return new MRTableShardBase(shardName, this, owner) {
            @Override
            public boolean isAvailable() {
              return true;
            }

            @Override
            public String crc() {
              return size;
            }
          };
        }
      }
      return new MRTableShardBase(shardName, this, owner) {
        @Override
        public boolean isAvailable() {
          return false;
        }

        @Override
        public String crc() {
          throw new IllegalStateException("crc is not available for missing tables");
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  @Override
  public MRTableShard resolve(final String path) {
    Computable<String, MRTable> RESOLVER = new Computable<String, MRTable>() {
      @Override
      public MRTable compute(final String argument) {
        if (argument.startsWith("user_sessions/")) {
          final Date date = parseDate(argument.subSequence(argument.indexOf("/") + 1, argument.length()));
          return new DailyMRTable("user_sessions", new MessageFormat("user_sessions/{0,date,yyyyMMdd}"), date, date);
        }
        return new FixedMRTable(argument);
      }

      private Date parseDate(final CharSequence sequence) {
        final int year = Integer.parseInt(sequence.subSequence(0, 4).toString());
        final int month = Integer.parseInt(sequence.subSequence(4, 6).toString());
        final int day = Integer.parseInt(sequence.subSequence(6, 8).toString());
        final Calendar calendar = Calendar.getInstance();
        //noinspection MagicConstant
        calendar.set(year, month - 1, day - 1);
        return calendar.getTime();
      }
    };

    return shard(path, RESOLVER.compute(path));
  }

  public boolean execute(final Class<? extends MRRoutine> routineClass, MRState state, final MRTable[] in, final MRTable[] out, final MRErrorsHandler errorsHandler) {
    final List<String> options = defaultOptions();

    int inputShardsCount = 0;
    int outputShardsCount = 0;
    for(int i = 0; i < in.length; i++) {
      for (MRTableShard shard : shards(in[i])) {
        options.add("-src");
        options.add(shard.path());
        inputShardsCount++;
      }
    }
    for(int i = 0; i < out.length; i++) {
      for (MRTableShard shard : shards(out[i])) {
        options.add("-dst");
        options.add(shard.path());
        outputShardsCount++;
      }
    }
    final String errorsShardName = "tmp/errors-" + Integer.toHexString(new FastRandom().nextInt());
    final MRTableShard errorsShard = shard(errorsShardName, new FixedMRTable(errorsShardName));
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
      options.add("-file");
      jarFile = jarBuilder.build(this);
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
          errorsHandler.error(table, key, subkey, value);
        }
        System.out.println(arg);
      }
    }, null);
    read(errorsShard, new MRRoutine(new String[]{errorsShardName}, null, state) {
      @Override
      public void invoke(final MRRoutine.Record record) {
        CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[2]);
        errorsHandler.error(record.key, record.sub, parts[0].toString(), parts[1]);
      }
    });
    delete(errorsShard);

    return errorsCount[0] == 0;
  }

  @Override
  public boolean execute(final Class<? extends MRRoutine> exec, final MRState state, final MRTable in, final MRTable... out) {
    return execute(exec, state, new MRTable[]{in}, out, null);
  }

  @Override
  public String name() {
    return "YaMR://" + master + "/";
  }
}
