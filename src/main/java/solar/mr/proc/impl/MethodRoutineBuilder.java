package solar.mr.proc.impl;

import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.MRRoutineBuilder;
import solar.mr.proc.State;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;

/**
* User: solar
* Date: 27.12.14
* Time: 13:42
*/
class MethodRoutineBuilder extends MRRoutineBuilder {
  private Class<?> routineClass;
  private String methodName;
  private RoutineType type;

  @Override
  public RoutineType getRoutineType() {
    return type;
  }

  @Override
  public MRRoutine build(MROutput output) {
    complete();
    try {
      final Object instance = routineClass.getConstructor(State.class);
      switch (type) {
        case MAP: {
          final Method method = routineClass.getMethod(methodName, String.class, String.class, CharSequence.class);
          return new MRMap(input(), output, state) {
            @Override
            public void map(String key, String sub, CharSequence value) {
              try {
                method.invoke(instance, key, sub, value);
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
        case REDUCE: {
          final Method method = routineClass.getMethod(methodName, String.class, Iterator.class);
          return new MRReduce(input(), output, state) {
            @Override
            public void reduce(String key, Iterator<MRRecord> reduce) {
              try {
                method.invoke(instance, key, reduce);
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    throw new UnsupportedOperationException("Should never happen");
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    complete();
    out.writeUTF(routineClass.getName());
    out.writeUTF(methodName);
    out.writeUTF(type.name());
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    routineClass = Class.forName(in.readUTF());
    methodName = in.readUTF();
    type = RoutineType.valueOf(in.readUTF());
    complete();
  }

  private void readObjectNoData() throws ObjectStreamException {
  }

  public void setRoutineClass(Class<?> routineClass) {
    this.routineClass = routineClass;
  }

  public void setType(RoutineType type) {
    this.type = type;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }
}