package com.expleague.lyadzhin.report.cfg;

import com.expleague.lyadzhin.report.sources.SourceRequest;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 08.04.15 20:40
*/
class ConfigurationPublisherJoba implements Joba {
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
