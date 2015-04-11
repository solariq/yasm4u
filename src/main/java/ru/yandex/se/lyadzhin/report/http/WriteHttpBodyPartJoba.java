package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class WriteHttpBodyPartJoba implements Joba {
  private final int partNum;
  private final UserHttpCommunicationDomain communicationDomain;

  public WriteHttpBodyPartJoba(HttpResponse httpResponse,
                               int partNum,
                               UserHttpCommunicationDomain communicationDomain)
  {
    this.partNum = partNum;
    this.communicationDomain = communicationDomain;
  }

  @Override
  public Ref[] consumes() {
    return partNum > 0 ? new Ref[]{new BodyPartDoneRef(partNum - 1)} : new Ref[0];
  }

  @Override
  public Ref[] produces() {
    return new Ref[]{new BodyPartDoneRef(partNum)};
  }

  @Override
  public void run() {
    System.out.println("Writing HTTP body, part = " + partNum +
            ", content = " + communicationDomain.getPartContent(partNum));
  }
}
