package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:17
*/
public class SourceResponse implements Ref<String, SourceCommunicationDomain> {
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
