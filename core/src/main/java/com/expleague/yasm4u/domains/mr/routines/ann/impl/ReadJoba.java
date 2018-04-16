package com.expleague.yasm4u.domains.mr.routines.ann.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;

import com.expleague.commons.util.ArrayTools;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.domains.wb.StateRef;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

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
    this.input = ArrayTools.map(input, MRPath.class, controller::resolve);
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
    final Thread readTh = new Thread(() -> {
      try {
        for (Ref ref : input) {
          if (ref instanceof MRPath) {
            env.read((MRPath) ref, arg -> {
                try {
                    seqs.put(arg);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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
    });
    readTh.start();
    try {
      final Constructor<?> constructor = method.getDeclaringClass().getConstructor(State.class);
      wb.set(output, method.invoke(constructor.newInstance(wb.snapshot()), new Iterator<MRRecord>() {
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
