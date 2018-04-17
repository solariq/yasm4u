package com.expleague.yasm4u.domains.mr.routines.ann.impl;

import com.expleague.commons.util.CollectionTools;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.wb.TempRef;
import com.expleague.yasm4u.domains.wb.Whiteboard;

import java.lang.reflect.Method;
import java.util.stream.Stream;

/**
* User: solar
* Date: 27.12.14
* Time: 13:02
*/
public class RoutineJoba implements Joba {
  public final Domain.Controller controller;
  public final Ref[] input;
  public final Ref[] output;
  public final Method method;
  public final MRRoutineBuilder.RoutineType type;

  public RoutineJoba(Domain.Controller controller, final Ref[] input, final Ref[] output, final Method method, MRRoutineBuilder.RoutineType type) {
    this.controller = controller;
    //noinspection unchecked
    this.input = input;
    this.output = output;
    this.method = method;
    this.type = type;
  }

  @Override
  public void run() {
    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.addInput(Stream.of(input).filter(CollectionTools.instanceOf(MRPath.class)).map(CollectionTools.cast(MRPath.class)).toArray(MRPath[]::new));
    builder.addOutput(Stream.of(output).filter(CollectionTools.instanceOf(MRPath.class)).map(CollectionTools.cast(MRPath.class)).toArray(MRPath[]::new));
    builder.setState(controller.domain(Whiteboard.class).snapshot());
    builder.setMethodName(method.getName());
    builder.setType(type);
    builder.setRoutineClass(method.getDeclaringClass());
    DefaultMRErrorsHandler errorsHandler = new DefaultMRErrorsHandler();
    controller.domain(MREnv.class).execute(builder, errorsHandler);
    if (errorsHandler.errorsCount() > 0) {
      throw new RuntimeException(errorsHandler.first());
    }
  }

  @Override
  public Ref[] consumes() {
    return input;
  }

  @Override
  public Ref[] produces() {
    return output;
  }

  @Override
  public String toString() {
    return "Annotated method routine: " + method.getDeclaringClass().getName() + " " + method.getName();
  }

}
