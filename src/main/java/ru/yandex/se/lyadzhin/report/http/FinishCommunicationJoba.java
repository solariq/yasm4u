package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

import java.util.ArrayList;
import java.util.List;

/**
* User: lyadzhin
* Date: 08.04.15 19:13
*/
class FinishCommunicationJoba implements Joba {
  private final HttpResponse httpResponse;
  private final int partsCount;
  private final UserHttpCommunicationDomain communicationDomain;

  public FinishCommunicationJoba(HttpResponse httpResponse,
                                 int partsCount,
                                 UserHttpCommunicationDomain communicationDomain)
  {
    this.httpResponse = httpResponse;
    this.partsCount = partsCount;
    this.communicationDomain = communicationDomain;
  }

  @Override
  public Ref[] consumes() {
    final List<Ref> result = new ArrayList<>();
    for (int i = 0; i < partsCount; i++)
      result.add(new BodyPartDoneRef(i));
    return result.toArray(new Ref[result.size()]);
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {UserHttpCommunicationDomain.REF_COMMUNICATION_STATUS};
  }

  @Override
  public void run() {
    System.out.println("Finishing communication");
    communicationDomain.setCommunicationStatus(UserHttpCommunicationDomain.CommunicationStatus.OK);
  }
}
