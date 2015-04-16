package ru.yandex.se.lyadzhin.report.cfg;

import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class ConfigurationPublisherJoba extends Joba.Stub {
  private final ConfigurationDomain configurationDomain;

  public ConfigurationPublisherJoba(ConfigurationDomain configurationDomain) {
    this.configurationDomain = configurationDomain;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {new SourceRequest(SourceRequest.SOURCE_FOO).response()};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {ConfigurationDomain.REF_CONFIGURATION};
  }

  @Override
  public void run() {
    System.out.println("Setting up configuration");
    configurationDomain.setup(new MockConfiguration());
  }

}
