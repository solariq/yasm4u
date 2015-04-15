package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
* User: lyadzhin
* Date: 08.04.15 19:13
*/
class FinishCommunicationJoba implements Joba {
  private final Whiteboard whiteboard;

  public FinishCommunicationJoba(HttpResponse httpResponse, Whiteboard whiteboard) {
    this.whiteboard = whiteboard;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[0];
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {UserHttpCommunicationDomain.Output.COMMUNICATION_STATUS};
  }

  @Override
  public void run() {
    System.out.println("Finishing communication");
    whiteboard.set(UserHttpCommunicationDomain.Output.COMMUNICATION_STATUS, UserHttpCommunicationDomain.CommunicationStatus.OK);
  }
}
