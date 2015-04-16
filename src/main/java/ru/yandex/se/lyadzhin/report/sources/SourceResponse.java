package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:17
*/
public class SourceResponse implements Ref<CharSequence, SourceCommunicationDomain> {
  private final SourceRequest request;

  public SourceResponse(SourceRequest request) {
    this.request = request;
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<CharSequence> type() {
    return CharSequence.class;
  }

  @Override
  public Class<SourceCommunicationDomain> domainType() {
    return SourceCommunicationDomain.class;
  }

  @Override
  public String resolve(SourceCommunicationDomain dom) {
    return dom.hasResponse(this) ? "PREVED" : null;
  }

  @Override
  public boolean available(SourceCommunicationDomain dom) {
    return dom.hasResponse(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SourceResponse that = (SourceResponse) o;
    return request.equals(that.request);
  }

  @Override
  public int hashCode() {
    return request.hashCode();
  }
}
