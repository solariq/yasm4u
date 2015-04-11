package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.HttpBodyPartRef;
import ru.yandex.se.lyadzhin.report.http.StartResponseRef;
import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 11.04.15 13:00
*/
class GenerateHttpBodyPartFromViewportJoba implements Joba {
  private final ViewportsDomain viewportsDomain;
  private final UserHttpCommunicationDomain communicationDomain;

  public GenerateHttpBodyPartFromViewportJoba(ViewportsDomain viewportsDomain, UserHttpCommunicationDomain communicationDomain) {
    this.viewportsDomain = viewportsDomain;
    this.communicationDomain = communicationDomain;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {ViewportsDomain.REF_VIEWPORTS_MODEL};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new StartResponseRef(1), new HttpBodyPartRef(0)};
  }

  @Override
  public void run() {
    communicationDomain.addBodyPartContent(0, viewportsDomain.getViewportsModel().toString());
  }
}
