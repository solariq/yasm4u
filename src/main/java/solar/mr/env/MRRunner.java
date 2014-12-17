package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.Pair;
import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.RuntimeInterruptedException;
import solar.mr.proc.State;
import solar.mr.MRTableShard;

/**
* User: solar
* Date: 23.09.14
* Time: 10:41
*/
public class MRRunner implements Runnable {
  public static final Pair<Integer, CharSequence> STOP = new Pair<Integer, CharSequence>(-1, "");
  public static final String TABLES_RESOURCE_NAME = ".tables";
  public static final String STATE_RESOURCE_NAME = ".state";
  public static final String ROUTINES_PROPERTY_NAME = "var:routines";

  private final Map<AccessType, List<String>> tables = new HashMap<>();
  private final Class<? extends MRRoutine> routine;
  private final State state;
  private final Reader in;
  private final boolean profilingMode;

  public Class<? extends MRRoutine> routine() {
    return routine;
  }

  public State state() {
    return state;
  }

  public enum AccessType {
    READ, WRITE
  }

  public MRRunner(char[] className) {
    this.in = new InputStreamReader(System.in, Charset.forName("UTF-8"));

    final String mrClass = new String(className);
    try {
      //noinspection unchecked
      routine = (Class<? extends MRRoutine>)Class.forName(mrClass);
      final ClassLoader loader = getClass().getClassLoader();
      final ObjectInputStream is = new ObjectInputStream(MRRunner.class.getResourceAsStream("/" + STATE_RESOURCE_NAME)){
        @Override
        protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          try {
            return loader.loadClass(desc.getName());
          }
          catch (ClassNotFoundException cnfe) {
            return super.resolveClass(desc);
          }
        }
      };
      state = (State)is.readObject();
      Boolean boxedValue = state.get(ProfilerMREnv.PROFILER_ENABLED_VAR);
      profilingMode = boxedValue == null ? false : boxedValue;
      CharSeqTools.processLines(new InputStreamReader(MRRunner.class.getResourceAsStream("/" + TABLES_RESOURCE_NAME), StreamTools.UTF), new Processor<CharSequence>() {
        @Override
        public void process(final CharSequence arg) {
          final CharSequence[] parts = CharSeqTools.split(arg, '\t');
          final AccessType accessType = AccessType.valueOf(parts[0].toString().toUpperCase());
          List<String> tables = MRRunner.this.tables.get(accessType);
          if (tables == null)
            MRRunner.this.tables.put(accessType, tables = new ArrayList<>());
          tables.add(parts[1].toString());
        }
      });

    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    final MROutputImpl out = new MROutputImpl(new OutputStreamWriter(System.out, StreamTools.UTF), tables.get(AccessType.WRITE).size());
    try {
      final Constructor<? extends MRRoutine> constructor = routine.getConstructor(String[].class, MROutput.class, State.class);
      constructor.setAccessible(true);
      final MRRoutine instance = constructor.newInstance(ArrayTools.toArray(tables.get(AccessType.READ)), out, state);

      long start = System.currentTimeMillis();
      CharSeqTools.processLines(in, instance);
      instance.process(CharSeq.EMPTY);
      if (profilingMode) {
        out.hostStatistics(InetAddress.getLocalHost().getHostName(), (int) (System.currentTimeMillis() - start));
      }
    } catch (RuntimeInterruptedException e) {
      // skip
    } catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IOException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } finally {
      out.interrupt();
      out.join();
    }
  }

  public void runLocally(char[] localMRHome, char[][] inTables, char[][] outTables) throws NoSuchMethodException,
                                                                                           InvocationTargetException,
                                                                                           InstantiationException,
                                                                                           IllegalAccessException
  {
    final LocalMREnv sampleEnv = new LocalMREnv(new String(localMRHome));
    final MRTableShard[] input = new MRTableShard[inTables.length];
    final MRTableShard[] output = new MRTableShard[outTables.length];
    for (int i = 0; i < input.length; i++) {
      input[i] = sampleEnv.resolve(new String(inTables[i]));
    }
    for (int i = 0; i < output.length; i++) {
      output[i] = sampleEnv.resolve(new String(outTables[i]));
    }
    sampleEnv.execute(routine(), state(), input, output, null);
  }

  public static void main(String[] args) {
    new MRRunner(args[0].toCharArray()).run();
  }
}
