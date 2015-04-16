package ru.yandex.se.lyadzhin.report.cfg;


import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.net.URI;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:42
 */
public class ConfigurationDomain implements Domain {
  static final Ref<Configuration, ConfigurationDomain> REF_CONFIGURATION = new ConfigurationRef();

  private Configuration configuration;

  public ConfigurationDomain() {
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    jobs.add(new InitialRequestsPublisherJoba());
    jobs.add(new ConfigurationPublisherJoba(this));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public void setup(Configuration configuration) {
    this.configuration = configuration;
  }

  public Ref<Configuration, ConfigurationDomain> goal() {
    return REF_CONFIGURATION;
  }

  private static class ConfigurationRef implements Ref<Configuration, ConfigurationDomain> {
    @Override
    public URI toURI() {
      return null;
    }

    @Override
    public Class<Configuration> type() {
      return Configuration.class;
    }

    @Override
    public Class<ConfigurationDomain> domainType() {
      return ConfigurationDomain.class;
    }

    @Override
    public Configuration resolve(ConfigurationDomain controller) {
      return controller.configuration;
    }

    @Override
    public boolean available(ConfigurationDomain controller) {
      return controller.configuration != null;
    }
  }
}
