package com.expleague.yasm4u.domains.mr.routines.ann.impl;

import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.util.ArrayList;
import java.util.List;

/**
* User: solar
* Date: 19.02.15
* Time: 23:38
*/
public class DefaultMRErrorsHandler implements MRErrorsHandler {
  final List<Throwable> errors = new ArrayList<>();
  @Override
  public void error(String type, String cause, MRRecord record) {
    RuntimeException th = new RuntimeException("Error during record processing! type: " + type + ", cause: " + cause + ", record: [" + record.toString() + "]");
    errors.add(th);
    throw th;
  }

  @Override
  public void error(Throwable th, MRRecord record) {
    errors.add(th);
    if (th instanceof RuntimeException)
      throw (RuntimeException)th;
    throw new RuntimeException(th);
  }

  @Override
  public int errorsCount() {
    return errors.size();
  }

  public Throwable first() {
    return errors.isEmpty() ? null : errors.get(0);
  }
}
