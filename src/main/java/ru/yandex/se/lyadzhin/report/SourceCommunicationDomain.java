package ru.yandex.se.lyadzhin.report;

import ru.yandex.se.yasm4u.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:46
 */
public class SourceCommunicationDomain implements Domain {
  static final String SOURCE_FOO = "FOO";

  public void request(String sourceKey) {

  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    routines.add(new PublishSourceRequestExecutorsRoutine());
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public static class SourceRequest implements Ref<RequestStatus, SourceCommunicationDomain> {
    private final SourceResponse sourceResponse;

    public SourceRequest(String sourceKey) {
      sourceResponse = new SourceResponse(this);
    }

    @Override
    public URI toURI() {
      return null;
    }

    @Override
    public Class<RequestStatus> type() {
      return RequestStatus.class;
    }

    @Override
    public Class<SourceCommunicationDomain> domainType() {
      return SourceCommunicationDomain.class;
    }

    @Override
    public RequestStatus resolve(SourceCommunicationDomain dom) {
      return RequestStatus.OK;
    }

    @Override
    public boolean available(SourceCommunicationDomain dom) {
      return true;
    }

    public SourceResponse response() {
      return sourceResponse;
    }
  }

  public static class SourceResponse implements Ref<String, SourceCommunicationDomain> {
    public SourceResponse(SourceRequest request) {
    }

    @Override
    public URI toURI() {
      return null;
    }

    @Override
    public Class<String> type() {
      return String.class;
    }

    @Override
    public Class<SourceCommunicationDomain> domainType() {
      return SourceCommunicationDomain.class;
    }

    @Override
    public String resolve(SourceCommunicationDomain dom) {
      return "PREVED";
    }

    @Override
    public boolean available(SourceCommunicationDomain dom) {
      return true;
    }
  }

  public static enum RequestStatus {
    OK, FAILED
  }

  private static class PublishSourceRequestExecutorsRoutine implements Routine {
    @Override
    public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
      final List<Joba> result = new ArrayList<>();
      for (final Ref ref : state) {
        if (ref instanceof SourceRequest) {
          final SourceRequest sourceRequest = (SourceRequest) ref;
          result.add(new Joba() {
            @Override
            public Ref[] consumes() {
              return new Ref[] {sourceRequest};
            }

            @Override
            public Ref[] produces() {
              return new Ref[] {sourceRequest.response()};
            }

            @Override
            public void run() {
              System.out.println("Processing source request to " + sourceRequest);
            }
          });
        }
      }
      return result.toArray(new Joba[result.size()]);
    }
  }
}
