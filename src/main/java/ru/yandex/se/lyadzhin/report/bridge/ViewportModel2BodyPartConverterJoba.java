package ru.yandex.se.lyadzhin.report.bridge;

import com.spbsu.commons.seq.CharSeqBuilder;
import ru.yandex.se.lyadzhin.report.http.BodyPartRef;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class ViewportModel2BodyPartConverterJoba implements Joba {
  private final BodyPartRef bodyPartRef;
  private final ViewportsDomain viewportsDomain;
  private final Whiteboard whiteboard;

  public ViewportModel2BodyPartConverterJoba(BodyPartRef bodyPartRef, ViewportsDomain viewportsDomain, Whiteboard whiteboard) {
    this.bodyPartRef = bodyPartRef;
    this.viewportsDomain = viewportsDomain;
    this.whiteboard = whiteboard;
  }

  @Override
  public Ref[] consumes() {
     return new Ref[] {ViewportsDomain.REF_VIEWPORTS_MODEL};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {bodyPartRef} ;
  }

  @Override
  public void run() {
    System.out.println("VP model =" + viewportsDomain.getViewportsModel());
    final CharSeqBuilder charSeqBuilder = new CharSeqBuilder().append("Hello world!");
    whiteboard.set(bodyPartRef, charSeqBuilder);
  }
}
