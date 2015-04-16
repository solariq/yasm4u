package ru.yandex.se.lyadzhin.report.cfg;

import ru.yandex.se.lyadzhin.report.SearchWhiteboard;
import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class InitialRequestsPublisherJoba extends Joba.Stub {
  public InitialRequestsPublisherJoba() {
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {SearchWhiteboard.USER_YANDEX_UID, SearchWhiteboard.USER_QUERY_TEXT};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new SourceRequest(SourceRequest.SOURCE_FOO)};
  }

  @Override
  public void run() {
    System.out.println("Generating FOO source request");
  }
}
