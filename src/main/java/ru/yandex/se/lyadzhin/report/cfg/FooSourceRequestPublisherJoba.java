package ru.yandex.se.lyadzhin.report.cfg;

import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class FooSourceRequestPublisherJoba implements Joba {
  private final SourceRequest fooSourceRequest;

  public FooSourceRequestPublisherJoba(SourceRequest fooSourceRequest) {
    this.fooSourceRequest = fooSourceRequest;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {ConfigurationDomain.Input.YANDEX_UID, ConfigurationDomain.Input.TEXT};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {fooSourceRequest};
  }

  @Override
  public void run() {
    System.out.println("Generation FOO source request");
  }
}
