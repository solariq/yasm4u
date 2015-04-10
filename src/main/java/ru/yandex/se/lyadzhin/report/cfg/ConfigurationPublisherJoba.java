package ru.yandex.se.lyadzhin.report.cfg;

import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

import java.util.Arrays;
import java.util.List;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class ConfigurationPublisherJoba implements Joba {
  private final ConfigurationDomain configurationDomain;
  private final SourceRequest fooSourceRequest;

  public ConfigurationPublisherJoba(ConfigurationDomain configurationDomain,
                                    SourceRequest fooSourceRequest)
  {
    this.configurationDomain = configurationDomain;
    this.fooSourceRequest = fooSourceRequest;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[] {fooSourceRequest.response()};
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {ConfigurationDomain.REF_CONFIGURATION};
  }

  @Override
  public void run() {
    System.out.println("Setting up configuration");
    configurationDomain.setup(new Configuration() {
      @Override
      public List<String> viewportIdList() {
        return Arrays.asList("web", "images", "movies");
      }
    });
  }
}
