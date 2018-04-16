package com.expleague.yasm4u;

import java.util.List;
import java.util.concurrent.Future;

/**
 * User: solar
 * Date: 19.03.15
 * Time: 21:01
 */
public interface JobExecutorService extends Domain.Controller {
  void addRoutine(Routine routine);
  void addJoba(Joba joba);

  <T> Future<T> calculate(Ref<T, ?> goal);
  Future<List<?>> calculate(Ref<?, ?>... goals);

  void addListener(ProgressListener listener);

  interface ProgressListener {
    void jobStart(Joba joba);
    void jobFinish(Joba joba);
    void jobException(Exception e, Joba joba);
  }
}
