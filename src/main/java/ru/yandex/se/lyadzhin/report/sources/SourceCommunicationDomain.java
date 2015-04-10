package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.*;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:46
 */
public class SourceCommunicationDomain implements Domain {
  public static final String SOURCE_FOO = "FOO";

  public void request(String sourceKey) {

  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    routines.add(new PublishSourceRequestExecutorsRoutine());
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public static enum RequestStatus {
    OK, FAILED
  }

}
