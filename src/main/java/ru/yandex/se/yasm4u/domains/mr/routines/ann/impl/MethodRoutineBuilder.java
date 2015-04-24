package ru.yandex.se.yasm4u.domains.mr.routines.ann.impl;

import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.ops.MRMap;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.MRReduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;

/**
* User: solar
* Date: 27.12.14
* Time: 13:42
*/
public class MethodRoutineBuilder extends MRRoutineBuilder {
  private Class<?> routineClass;
  private String methodName;
  private RoutineType type;

  @Override
  public RoutineType getRoutineType() {
    return type;
  }

  @Override
  public MROperation build(final MROutput output) {
    complete();
    Constructor<?> constructor;
    try {
      constructor = routineClass.getConstructor(State.class);
    } catch (NoSuchMethodException e) {
      try {
        constructor = routineClass.getConstructor();
      } catch (NoSuchMethodException e1) {
        throw new RuntimeException("Unable to find proper constructor in " + routineClass.getName());
      }
    }
    try {

      final Object instance = constructor.newInstance(state);
      switch (type) {
        case MAP: {
          try {
            final Method method = routineClass.getMethod(methodName, MRPath.class, String.class, String.class, CharSequence.class, MROutput.class);
            return new MRMap(input(), output, state) {
              @Override
              public void map(MRPath table, String sub, CharSequence value, String key) {
                try {
                  method.invoke(instance, table, key, sub, value, output);
                } catch (IllegalAccessException | InvocationTargetException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
          catch (NoSuchMethodException nsme) {
            final Method shortMethod = routineClass.getMethod(methodName, String.class, String.class, CharSequence.class, MROutput.class);
            return new MRMap(input(), output, state) {
              @Override
              public void map(MRPath table, String sub, CharSequence value, String key) {
                try {
                  shortMethod.invoke(instance, key, sub, value, output);
                } catch (IllegalAccessException | InvocationTargetException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
        }
        case REDUCE: {
          final Method method = routineClass.getMethod(methodName, String.class, Iterator.class, MROutput.class);
          return new MRReduce(input(), output, state) {
            @Override
            public void reduce(String key, Iterator<MRRecord> reduce) {
              try {
                method.invoke(instance, key, reduce, output);
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
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

  @Override
  public String toString() {
    return routineClass.getName() + " " + type.toString() + " " + methodName;
  }
}
