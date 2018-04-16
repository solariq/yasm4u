package com.expleague.lyadzhin.report.http;

import com.expleague.lyadzhin.report.cfg.ConfigurationDomain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.domains.wb.Whiteboard;

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
    return new Ref[]{ConfigurationDomain.Input.YANDEX_UID, ConfigurationDomain.Input.TEXT};
  }

  @Override
  public void run() {
    System.out.println("Parsing HTTP request");
    whiteboard.set(ConfigurationDomain.Input.YANDEX_UID, "12345");
    whiteboard.set(ConfigurationDomain.Input.TEXT, "kotiki");
  }
}
