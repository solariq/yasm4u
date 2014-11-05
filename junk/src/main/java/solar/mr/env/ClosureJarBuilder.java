package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import com.spbsu.commons.func.Action;
import solar.mr.MREnv;
import solar.mr.MRRoutine;
import solar.mr.MRTable;
import solar.mr.MRTools;
import solar.mr.proc.MRState;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 20.10.14
 * Time: 11:18
 */
public class ClosureJarBuilder {
  private final String localMRHome;
  private Class<? extends MRRoutine> routine;
  private final List<MRTableShard> input = new ArrayList<>();
  private final List<MRTableShard> output = new ArrayList<>();
  final HashMap<String, byte[]> resourcesMap = new HashMap<>();

  public ClosureJarBuilder(String localMRHome) {
    this.localMRHome = localMRHome;
  }

  public File build(MREnv targetEnv) {
    final File tempFile;
    try {
      tempFile = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      tempFile.delete();
      tempFile.deleteOnExit();

      final LocalMREnv sampleEnv = new LocalMREnv(localMRHome);
      final char[][] inTables = new char[input.size()][];
      final char[][] outTables = new char[output.size()][];
      {
        final ByteArrayOutputStream tablesDump = new ByteArrayOutputStream();
        final PrintStream tablesOut = new PrintStream(tablesDump);
        for (int i = 0; i < input.size(); i++) {
          final MRTableShard shard = input.get(i);
          tablesOut.println(MRRunner.AccessType.READ.toString() + "\t" + shard.path());
          inTables[i] = sampleEnv.shards(shard.owner())[0].path().toCharArray();
        }

        for (int i = 0; i < output.size(); i++) {
          final MRTableShard shard = output.get(i);
          tablesOut.println(MRRunner.AccessType.WRITE.toString() + "\t" + shard.path());
          outTables[i] = sampleEnv.shards(shard.owner())[0].path().toCharArray();
        }
        addResource(MRRunner.TABLES_RESOURCE_NAME, tablesDump.toByteArray());
      }
      MRTools.buildClosureJar(MRRunner.class, tempFile.getAbsolutePath(), new Action<Class>() {
        @SuppressWarnings("unchecked")
        @Override
        public void invoke(final Class loadedClass) {
          final LocalMREnv sampleEnv = new LocalMREnv(new String(localMRHome.toCharArray()));
          final Constructor<MRRunner> constructor;
          try {
            constructor = loadedClass.getConstructor(char[].class);
            @SuppressWarnings("PrimitiveArrayArgumentToVariableArgMethod")
            final MRRunner instance = constructor.newInstance(routine.getName().toCharArray());
            final MRTable[] input = new MRTable[ClosureJarBuilder.this.input.size()];
            final MRTable[] output = new MRTable[ClosureJarBuilder.this.output.size()];
            for (int i = 0; i < input.length; i++) {
              input[i] = sampleEnv.resolve(new String(inTables[i])).owner();
            }
            for (int i = 0; i < output.length; i++) {
              output[i] = sampleEnv.resolve(new String(outTables[i])).owner();
            }
            sampleEnv.execute(instance.routine(), instance.state(), input, output, null);
          } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }, resourcesMap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tempFile;
  }

  public void addOutput(final MRTableShard mrTable) {
    input.add(mrTable);
  }

  public void addInput(final MRTableShard mrTable) {
    output.add(mrTable);
  }

  public void setRoutine(final Class<? extends MRRoutine> routine) {
    this.routine = routine;
  }

  public void setState(MRState state) {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final ObjectOutputStream oos = new ObjectOutputStream(out);
      oos.writeObject(state);
      addResource(MRRunner.STATE_RESOURCE_NAME, out.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  public void addResource(final String name, final byte[] content) {
    resourcesMap.put(name, content);
  }
}
