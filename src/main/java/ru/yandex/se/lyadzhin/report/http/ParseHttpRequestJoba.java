package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.lyadzhin.report.SearchWhiteboard;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
* User: lyadzhin
* Date: 08.04.15 19:12
*/
class ParseHttpRequestJoba extends Joba.Stub {
  private final HttpRequest request;
  private final Whiteboard whiteboard;

  public ParseHttpRequestJoba(HttpRequest request, Whiteboard whiteboard) {
    this.request = request;
    this.whiteboard = whiteboard;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[0];
  }

  @Override
  public Ref[] produces() {
    return new Ref[]{SearchWhiteboard.USER_YANDEX_UID, SearchWhiteboard.USER_QUERY_TEXT};
  }

  @Override
  public void run() {
    System.out.println("Parsing HTTP request");
    whiteboard.set(SearchWhiteboard.USER_YANDEX_UID, "12345");
    whiteboard.set(SearchWhiteboard.USER_QUERY_TEXT, "kotiki");
  }
}
