package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class WriteHttpBodyPartJoba implements Joba {
  private final BodyPartRef bodyPartRef;
  private final PartDoneRef partDoneRef;
  private final Whiteboard wb;

  public WriteHttpBodyPartJoba(HttpResponse httpResponse, BodyPartRef bodyPartRef, Whiteboard wb) {
    this.bodyPartRef = bodyPartRef;
    this.partDoneRef = new PartDoneRef(bodyPartRef);
    this.wb = wb;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[]{bodyPartRef};
  }

  @Override
  public Ref[] produces() {
    return new Ref[]{partDoneRef};
  }

  @Override
  public void run() {
    System.out.println("Writing HTTP body, part = " + bodyPartRef.partNum +
            ", content = " + bodyPartRef.resolve(wb).toString());
    wb.set(partDoneRef, true);
  }
}
