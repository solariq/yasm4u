package ru.yandex.se.lyadzhin.report.cfg;


import ru.yandex.se.lyadzhin.report.sources.SourceCommunicationDomain;
import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.wb.StateRef;

import java.net.URI;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:42
 */
public class ConfigurationDomain implements Domain {
  public static final Ref<Configuration, ConfigurationDomain> REF_CONFIGURATION = new ConfigurationRef();

  public interface Input {
    StateRef<String> YANDEX_UID = new StateRef<>("yandex_uid", String.class);
    StateRef<String> TEXT = new StateRef<>("text", String.class);
  }

  private Configuration configuration;

  public ConfigurationDomain() {
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    final SourceRequest fooSourceRequest =
            new SourceRequest(SourceCommunicationDomain.SOURCE_FOO);
    jobs.add(new FooSourceRequestPublisherJoba(fooSourceRequest));
    jobs.add(new ConfigurationPublisherJoba(this, fooSourceRequest));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public void setup(Configuration configuration) {
    this.configuration = configuration;
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
