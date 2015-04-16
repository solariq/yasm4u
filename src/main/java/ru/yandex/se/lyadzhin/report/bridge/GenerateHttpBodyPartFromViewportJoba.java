package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.HttpBodyPartRef;
import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.viewports.Viewport;
import ru.yandex.se.lyadzhin.report.viewports.ViewportRef;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 11.04.15 13:00
*/
class GenerateHttpBodyPartFromViewportJoba extends Joba.Stub {
  private final ViewportsDomain viewportsDomain;
  private final UserHttpCommunicationDomain communicationDomain;
  private final int bodyPartNum;
  private final String viewportId;

  public GenerateHttpBodyPartFromViewportJoba(ViewportsDomain viewportsDomain,
                                              UserHttpCommunicationDomain communicationDomain,
                                              int bodyPartNum, String viewportId)
  {
    this.viewportsDomain = viewportsDomain;
    this.communicationDomain = communicationDomain;
    this.bodyPartNum = bodyPartNum;
    this.viewportId = viewportId;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {new ViewportRef(viewportId)};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new HttpBodyPartRef(bodyPartNum)};
  }

  @Override
  public void run() {
    final Viewport viewport = viewportsDomain.findViewportById(viewportId);
    communicationDomain.addBodyPartContent(bodyPartNum, viewport != null ? viewport.toString() : null);
  }
}
