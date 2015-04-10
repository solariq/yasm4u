package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:17
*/
public class SourceRequest implements Ref<SourceCommunicationDomain.RequestStatus, SourceCommunicationDomain> {
  private final SourceResponse sourceResponse;
  private final String sourceKey;

  public SourceRequest(String sourceKey) {
    this.sourceKey = sourceKey;
    this.sourceResponse = new SourceResponse(this);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SourceRequest that = (SourceRequest) o;
    return sourceKey.equals(that.sourceKey);
  }

  @Override
  public int hashCode() {
    return sourceKey.hashCode();
  }
}
