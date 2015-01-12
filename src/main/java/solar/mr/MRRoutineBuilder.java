package solar.mr;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.env.MROutputImpl;
import solar.mr.env.MRRunner;
import solar.mr.proc.State;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: solar
 * Date: 25.12.14
 * Time: 15:25
 */
public abstract class MRRoutineBuilder implements Serializable {
  protected State state;
  private List<String> tablesIn = new ArrayList<>();
  private List<String> tablesOut = new ArrayList<>();
  private boolean complete = false;

  public void addInput(String... tables) {
    checkComplete();
    this.tablesIn.addAll(Arrays.asList(tables));
  }

  public void addOutput(String... tables) {
    checkComplete();
    this.tablesOut.addAll(Arrays.asList(tables));
  }

  public void setState(State state) {
    checkComplete();
    this.state = state;
  }

  private void checkComplete() {
    if (complete)
      throw new IllegalStateException("Can not modify complete builder");
  }

  public boolean complete() {
    boolean prevState = this.complete;
    this.complete = true;
    return prevState;
  }

  public abstract RoutineType getRoutineType();

//  {
//    final Class<? extends MRRoutine> routineClass = this.routineClass;
//    if (MRMap.class.isAssignableFrom(routineClass))
//      return RoutineType.MAP;
//
//    else if (MRReduce.class.isAssignableFrom(routineClass))
//      return RoutineType.REDUCE;
//    else
//      throw new RuntimeException("Unknown MR routine type");
//  }

  public abstract MRRoutine build(final MROutput output);
//  {
//    complete();
//    try {
//      if (Arrays.equals(tablesOut.toArray(new String[tablesOut.size()]), output.names()))
//        throw new IllegalArgumentException("Output tables of the routine does not match those from MROutput");
//      Constructor<? extends MRRoutine> constructor = routineClass.getConstructor(String[].class, MROutput.class, State.class);
//      return constructor.newInstance(tablesIn.toArray(new String[tablesIn.size()]), output, state);
//    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
//      throw new RuntimeException(e);
//    }
//  }

  private File jar;
  public File buildJar(MREnv env, MRErrorsHandler errorsHandler) {
    complete();
    if (jar != null)
      return jar;
    Process process = null;
    try {
      final File jar = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      jar.delete();
      jar.deleteOnExit();
      final Writer to = new OutputStreamWriter(process.getOutputStream(), StreamTools.UTF);
      final Reader from = new InputStreamReader(process.getInputStream(), StreamTools.UTF);
      to.append(CharSeqTools.toBase64(builderSerialized.toByteArray())).append("\n");
      to.flush();

      for (int i = 0; i < tablesIn.size(); i++) {
        final MRTableShard inputShard = env.resolve(tablesIn.get(i));
        env.sample(inputShard, new Processor<CharSequence>() {
          @Override
          public void process(CharSequence arg) {
            try {
              to.append(arg).append("\n");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      to.close();
      final MROutputImpl output = new MROutputImpl(env, output(), new MRErrorsHandler() {
        @Override
        public void error(String type, String cause, MRRecord record) {
          throw new RuntimeException("Error during MR operation.\nType: " + type + "\tCause: " + cause + "\tRecord: [" + record + "]");
        }

        @Override
        public void error(Throwable th, MRRecord record) {
          throw new RuntimeException("Exception during processing: [" + record.toString() + "]", th);
        }
      });
      CharSeqTools.processLines(from, new Processor<CharSequence>() {
        @Override
        public void process(CharSequence arg) {
          output.parse(arg);
        }
      });

      process.waitFor();
      return this.jar = jar;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    finally {
      if (process != null)
        try {
          StreamTools.transferData(process.getErrorStream(), System.err);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
    }
  }

  public String[] output() {
    complete();
    return tablesOut.toArray(new String[tablesOut.size()]);
  }

  public String[] input() {
    complete();
    return tablesIn.toArray(new String[tablesIn.size()]);
  }

  public enum RoutineType {
    MAP, REDUCE
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    complete();
    out.writeObject(state);
    out.writeObject(tablesIn);
    out.writeObject(tablesOut);
//    out.writeUTF(routineClass.getName());
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    state = (State)in.readObject();
    tablesIn = (List<String>) in.readObject();
    tablesOut = (List<String>) in.readObject();
//    routineClass = (Class<? extends MRRoutine>) Class.forName(in.readUTF());
    complete();
  }

  private void readObjectNoData() throws ObjectStreamException {
  }
}
