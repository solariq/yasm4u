package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:17
*/
public class SourceRequest implements Ref<SourceCommunicationDomain.RequestStatus, SourceCommunicationDomain> {
  private final SourceResponse sourceResponse;

  public SourceRequest(String sourceKey) {
    sourceResponse = new SourceResponse(this);
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<SourceCommunicationDomain.RequestStatus> type() {
    return SourceCommunicationDomain.RequestStatus.class;
  }

  @Override
  public Class<SourceCommunicationDomain> domainType() {
    return SourceCommunicationDomain.class;
  }

  @Override
  public SourceCommunicationDomain.RequestStatus resolve(SourceCommunicationDomain dom) {
    return SourceCommunicationDomain.RequestStatus.OK;
  }

  @Override
  public boolean available(SourceCommunicationDomain dom) {
    return true;
  }

  public SourceResponse response() {
    return sourceResponse;
  }
}
