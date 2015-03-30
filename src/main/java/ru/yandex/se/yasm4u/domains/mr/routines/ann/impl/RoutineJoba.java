package ru.yandex.se.yasm4u.domains.mr.routines.ann.impl;

import com.spbsu.commons.func.Computable;
import com.spbsu.commons.util.ArrayTools;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.lang.reflect.Method;

/**
* User: solar
* Date: 27.12.14
* Time: 13:02
*/
public class RoutineJoba implements Joba {
  public final JobExecutorService controller;
  public final Ref<? extends MRPath>[] input;
  public final Ref<? extends MRPath>[] output;
  public final Method method;
  public final MRRoutineBuilder.RoutineType type;

  public RoutineJoba(JobExecutorService controller, final Ref<? extends MRPath>[] input, final Ref<? extends MRPath>[] output, final Method method, MRRoutineBuilder.RoutineType type) {
    this.controller = controller;
    this.input = input;
    this.output = output;
    this.method = method;
    this.type = type;
  }

  @Override
  public void run() {
    final MRPath[] inputResolved = ArrayTools.map(input, MRPath.class, new Computable<Ref<? extends MRPath>, MRPath>() {
      @Override
      public MRPath compute(Ref<? extends MRPath> argument) {
        return argument.resolve(controller);
      }
    });
    final MRPath[] outputResolved = ArrayTools.map(output, MRPath.class, new Computable<Ref<? extends MRPath>, MRPath>() {
      @Override
      public MRPath compute(Ref<? extends MRPath> argument) {
        return argument.resolve(controller);
      }
    });
    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.addInput(inputResolved);
    builder.addOutput(outputResolved);
    builder.setState(controller.domain(Whiteboard.class).snapshot());
    builder.setMethodName(method.getName());
    builder.setType(type);
    builder.setRoutineClass(method.getDeclaringClass());
    controller.domain(MREnv.class).execute(builder, new DefaultMRErrorsHandler());
  }

  @Override
  public Ref<? extends MRPath>[] consumes() {
    return input;
  }

  @Override
  public Ref<? extends MRPath>[] produces() {
    return output;
  }

  @Override
  public String toString() {
    return "Annotated method routine: " + method.getDeclaringClass().getName() + " " + method.getName();
  }

}
