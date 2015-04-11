package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.lyadzhin.report.cfg.ConfigurationDomain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
* User: lyadzhin
* Date: 08.04.15 19:12
*/
class ParseHttpRequestJoba implements Joba {
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
    return new Ref[]{ConfigurationDomain.USER_YANDEX_UID, ConfigurationDomain.USER_QUERY_TEXT};
  }

  @Override
  public void run() {
    System.out.println("Parsing HTTP request");
    whiteboard.set(ConfigurationDomain.USER_YANDEX_UID, "12345");
    whiteboard.set(ConfigurationDomain.USER_QUERY_TEXT, "kotiki");
  }
}
