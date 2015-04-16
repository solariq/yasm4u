package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.StartResponseRef;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 13.04.15 9:59
*/
class InitiateResponseStartJoba extends Joba.Stub {
  private final int bodyPartsCount;

  public InitiateResponseStartJoba(int bodyPartsCount) {
    this.bodyPartsCount = bodyPartsCount;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[0];
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new StartResponseRef(bodyPartsCount)};
  }

  @Override
  public void run() {
  }
}
