package ru.yandex.se.lyadzhin.report.logs;

import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 14.04.15 22:17
 */
public class LoggingDomain implements Domain {
  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    // TODO: ng-request-log, ng-answer-log, access-log,
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }
}
