package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class WriteHttpBodyPartJoba extends Joba.Stub {
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
    return partNum > 0 ? new Ref[]{new HttpBodyPartRef(partNum), new BodyPartDoneRef(partNum - 1)} :
            new Ref[]{new HttpBodyPartRef(partNum)};
  }

  @Override
  public Ref[] produces() {
    return new Ref[]{new BodyPartDoneRef(partNum)};
  }

  @Override
  public void run() {
    final CharSequence partContent = communicationDomain.getPartContent(partNum);
    System.out.println(((partContent != null) ? "Writing" : "Skipping") + " HTTP body part = " + partNum + ", content = " + partContent);
  }
}
