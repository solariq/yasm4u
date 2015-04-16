package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.HttpBodyPartRef;
import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
 * User: lyadzhin
 * Date: 13.04.15 10:01
 */
public class GenerateHttpBodyPartFromModelJoba extends Joba.Stub {
  private final ViewportsDomain viewportsDomain;
  private final UserHttpCommunicationDomain communicationDomain;
  private final int bodyPartNum;

  public GenerateHttpBodyPartFromModelJoba(ViewportsDomain viewportsDomain,
                                           UserHttpCommunicationDomain communicationDomain,
                                           int bodyPartNum)
  {
    this.viewportsDomain = viewportsDomain;
    this.communicationDomain = communicationDomain;
    this.bodyPartNum = bodyPartNum;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {ViewportsDomain.REF_VIEWPORTS_MODEL};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new HttpBodyPartRef(bodyPartNum)};
  }

  @Override
  public void run() {
    communicationDomain.addBodyPartContent(bodyPartNum, viewportsDomain.getViewportsModel().toString());
  }
}
