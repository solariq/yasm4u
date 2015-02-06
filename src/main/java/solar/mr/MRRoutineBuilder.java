package solar.mr;

import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;

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
  private List<MRPath> tablesIn = new ArrayList<>();
  private List<MRPath> tablesOut = new ArrayList<>();
  private boolean complete = false;

  public void addInput(MRPath... paths) {
    checkComplete();
    this.tablesIn.addAll(Arrays.asList(paths));
  }

  public void addOutput(MRPath... paths) {
    checkComplete();
    this.tablesOut.addAll(Arrays.asList(paths));
  }

  public void setState(State state) {
    checkComplete();
    this.state = state;
  }

  protected void checkComplete() {
    if (complete)
      throw new IllegalStateException("Can not modify complete builder");
  }

  protected boolean complete() {
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

  public MRPath[] output() {
    complete();
    return tablesOut.toArray(new MRPath[tablesOut.size()]);
  }

  public MRPath[] input() {
    complete();
    return tablesIn.toArray(new MRPath[tablesIn.size()]);
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
    tablesIn = (List<MRPath>) in.readObject();
    tablesOut = (List<MRPath>) in.readObject();
//    routineClass = (Class<? extends MRRoutine>) Class.forName(in.readUTF());
    complete();
  }

  private void readObjectNoData() throws ObjectStreamException {
  }
}
