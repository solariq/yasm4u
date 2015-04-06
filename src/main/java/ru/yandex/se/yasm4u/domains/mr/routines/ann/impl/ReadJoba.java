package ru.yandex.se.yasm4u.domains.mr.routines.ann.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.func.Computable;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.util.ArrayTools;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

/**
* User: solar
* Date: 13.10.14
* Time: 8:04
*/
public class ReadJoba implements Joba {
  private final MRPath[] input;
  private final StateRef output;
  private final MREnv env;
  private final Method method;
  private final Whiteboard wb;

  public ReadJoba(final Domain.Controller controller, Ref<? extends MRPath, ?>[] input, StateRef<?> output, Method method) {
    //noinspection unchecked
    this.input = ArrayTools.map(input, MRPath.class, new Computable<Ref<? extends MRPath, ?>, MRPath>() {
      @Override
      public MRPath compute(Ref<? extends MRPath, ?> argument) {
        return controller.resolve(argument);
      }
    });
    this.output = output;
    this.env = controller.domain(MREnv.class);
    this.wb = controller.domain(Whiteboard.class);
    this.method = method;
  }

  @Override
  public String toString() {
    return "Read " + Arrays.toString(input) + " -> " + method.toString() + " -> " + output;
  }

  @Override
  public void run() {
    final ArrayBlockingQueue<MRRecord> seqs = new ArrayBlockingQueue<>(1000);
    final Thread readTh = new Thread(){
      @Override
      public void run() {
        try {
          for (Ref ref : input) {
            if (ref instanceof MRPath) {
              env.read((MRPath) ref, new Processor<MRRecord>() {
                  @Override
                  public void process(final MRRecord arg) {
                      try {
                          seqs.put(arg);
                      } catch (InterruptedException e) {
                          throw new RuntimeException(e);
                      }
                  }
              });
            }
          }
          seqs.put(MRRecord.EMPTY);
        }
        catch (RuntimeException re) {
          if (re.getCause() instanceof InterruptedException)
            return;
          throw re;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    readTh.start();
    try {
      final Constructor<?> constructor = method.getDeclaringClass().getConstructor(State.class);
      wb.set(output.name, method.invoke(constructor.newInstance(wb.snapshot()), new Iterator<MRRecord>() {
        MRRecord next = null;
        boolean needs2wait = true;

        @Override
        public boolean hasNext() {
          if (next == null && needs2wait) {
            try {
              next = seqs.take();
              if (next == MRRecord.EMPTY) {
                needs2wait = false;
                next = null;
              }
            } catch (InterruptedException e) {
              needs2wait = false;
            }
          }
          return next != null;
        }

        @Override
        public MRRecord next() {
          if (!hasNext())
            throw new NoSuchElementException();
          final MRRecord result = next;
          next = null;
          return result;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }));
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e) {
      throw new RuntimeException(e);
    }
    try {
      readTh.join();
    }
    catch (InterruptedException ie) {
      // skip
    }
  }

  @Override
  public Ref[] consumes() {
    return input;
  }

  @Override
  public Ref[] produces() {
    //noinspection unchecked
    return new Ref[] {output};
  }
}
