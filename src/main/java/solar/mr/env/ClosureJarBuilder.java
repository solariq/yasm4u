package solar.mr.env;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import com.spbsu.commons.func.Action;
import solar.mr.MRRoutine;
import solar.mr.MRTools;
import solar.mr.proc.State;
import solar.mr.MRTableShard;

/**
 * User: solar
 * Date: 20.10.14
 * Time: 11:18
 */
public class ClosureJarBuilder {
  private String localMRHome;
  private Class<? extends MRRoutine> routine;
  private final List<MRTableShard> input = new ArrayList<>();
  private final List<MRTableShard> output = new ArrayList<>();
  final HashMap<String, byte[]> resourcesMap = new HashMap<>();

  public ClosureJarBuilder(String localMRHome) {
    this.localMRHome = localMRHome;
  }

  public File build() {
    final File tempFile;
    try {
      tempFile = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      tempFile.delete();
      tempFile.deleteOnExit();

      final char[][] inTables = new char[input.size()][];
      final char[][] outTables = new char[output.size()][];
      {
        final ByteArrayOutputStream tablesDump = new ByteArrayOutputStream();
        final PrintStream tablesOut = new PrintStream(tablesDump);
        for (int i = 0; i < input.size(); i++) {
          final MRTableShard shard = input.get(i);
          tablesOut.println(MRRunner.AccessType.READ.toString() + "\t" + shard.path());
          inTables[i] = shard.path().toCharArray();
        }

        for (int i = 0; i < output.size(); i++) {
          final MRTableShard shard = output.get(i);
          tablesOut.println(MRRunner.AccessType.WRITE.toString() + "\t" + shard.path());
          outTables[i] = shard.path().toCharArray();
        }
        addResource(MRRunner.TABLES_RESOURCE_NAME, tablesDump.toByteArray());
      }
      MRTools.buildClosureJar(MRRunner.class, tempFile.getAbsolutePath(), new Action<Class>() {
        @SuppressWarnings({"PrimitiveArrayArgumentToVariableArgMethod", "unchecked"})
        @Override
        public void invoke(final Class loadedClass) {
          try {
            Constructor constructor = loadedClass.getConstructor(char[].class);
            Object runner = constructor.newInstance(routine.getName().toCharArray());
            final Method runLocally = loadedClass.getMethod("runLocally", char[].class, char[][].class, char[][].class);
            runLocally.invoke(runner, localMRHome.toCharArray(), inTables, outTables);
          } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }, resourcesMap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      resourcesMap.clear();
      input.clear();
      output.clear();
      routine = null;
    }
    return tempFile;
  }

  public void addOutput(final MRTableShard mrTable) {
    output.add(mrTable);
  }

  public void addInput(final MRTableShard mrTable) {
    input.add(mrTable);
  }

  public void setRoutine(final Class<? extends MRRoutine> routine) {
    this.routine = routine;
  }

  public void setState(State state) {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final ObjectOutputStream oos = new ObjectOutputStream(out);
      oos.writeObject(state);
      oos.close();
      addResource(MRRunner.STATE_RESOURCE_NAME, out.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  public void addResource(final String name, final byte[] content) {
    resourcesMap.put(name, content);
  }

  public void setLocalEnv(final LocalMREnv localEnv) {
    localMRHome = localEnv.home();
  }
}
