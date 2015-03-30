package ru.yandex.se.yasm4u.domains.mr.routines.ann.impl;

import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.lang.reflect.Method;

/**
* User: solar
* Date: 27.12.14
* Time: 13:02
*/
public class RoutineJoba implements Joba {
  public final Domain.Controller controller;
  public final MRPath[] input;
  public final MRPath[] output;
  public final Method method;
  public final MRRoutineBuilder.RoutineType type;

  public RoutineJoba(Domain.Controller controller, final Ref<? extends MRPath>[] input, final Ref<? extends MRPath>[] output, final Method method, MRRoutineBuilder.RoutineType type) {
    this.controller = controller;
    //noinspection unchecked
    this.input = new MRPath[input.length];
    for(int i = 0; i < input.length; i++) {
      final MRPath resolve = input[i].resolve(controller);
      this.input[i] = type == MRRoutineBuilder.RoutineType.REDUCE ? resolve.mksorted() : resolve;
    }
    this.output = new MRPath[output.length];
    for(int i = 0; i < output.length; i++) {
      final MRPath resolve = output[i].resolve(controller);
      this.output[i] = resolve;
    }
    this.method = method;
    this.type = type;
  }

  @Override
  public void run() {
    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.addInput(input);
    builder.addOutput(output);
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
