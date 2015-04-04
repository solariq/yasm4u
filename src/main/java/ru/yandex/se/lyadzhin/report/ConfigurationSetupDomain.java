package ru.yandex.se.lyadzhin.report;


import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.StateRef;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:42
 */
public class ConfigurationSetupDomain implements Domain {
  public interface Output {
    StateRef<Configuration> CONFIGURATION = new StateRef<>("configuration", Configuration.class);
  }

  @Override
  public void init(final JobExecutorService jes) {
    final SourceCommunicationDomain.SourceRequest fooSourceRequest =
            new SourceCommunicationDomain.SourceRequest(SourceCommunicationDomain.SOURCE_FOO);
    jes.addJoba(new FooSourceRequestPublisherJoba(fooSourceRequest));
    jes.addJoba(new ConfigurationPublisherJoba(fooSourceRequest));
  }

  private static class ConfigurationPublisherJoba implements Joba {
    private final SourceCommunicationDomain.SourceRequest fooSourceRequest;

    public ConfigurationPublisherJoba(SourceCommunicationDomain.SourceRequest fooSourceRequest) {
      this.fooSourceRequest = fooSourceRequest;
    }

    @Override
    public Ref<?>[] consumes() {
      return new Ref<?>[] {fooSourceRequest.response()};
    }

    @Override
    public Ref<?>[] produces() {
      return new Ref<?>[] {Output.CONFIGURATION};
    }

    @Override
    public void run() {
      System.out.println("Setting up configuration");
    }
  }

  private static class FooSourceRequestPublisherJoba implements Joba {
    private final SourceCommunicationDomain.SourceRequest fooSourceRequest;

    public FooSourceRequestPublisherJoba(SourceCommunicationDomain.SourceRequest fooSourceRequest) {
      this.fooSourceRequest = fooSourceRequest;
    }

    @Override
    public Ref<?>[] consumes() {
      return new Ref<?>[] {UserHttpCommunicationDomain.Output.YANDEX_UID, UserHttpCommunicationDomain.Output.TEXT};
    }

    @Override
    public Ref<?>[] produces() {
      return new Ref<?>[] {fooSourceRequest};
    }

    @Override
    public void run() {
      System.out.println("Generation FOO source request");
    }
  }
}
