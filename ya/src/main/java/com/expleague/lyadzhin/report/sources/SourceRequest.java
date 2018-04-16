package com.expleague.lyadzhin.report.sources;

import com.expleague.yasm4u.Ref;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
* User: lyadzhin
* Date: 08.04.15 19:17
*/
public class SourceRequest implements Ref<SourceCommunicationDomain.RequestStatus, SourceCommunicationDomain> {
  public static final Set<String> ALL_SOURCES = new HashSet<>(Arrays.asList(
          "IMAGES_SEARCH",
          "MOBILE_APPS_SEARCH",
          "WEB_SEARCH",
          "VIDEO_SEARCH",
          "GEO_SEARCH",
          "MAPS_ROUTE",
          "QUERY_WIZARD"
  ));
  public static final String SOURCE_FOO = "FOO";

  private final SourceResponse sourceResponse;
  private final String sourceKey;

  public SourceRequest(String sourceKey) {
    this.sourceKey = sourceKey;
    this.sourceResponse = new SourceResponse(this);
  }

  public String sourceKey() {
    return sourceKey;
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

  @Override
  public String toString() {
    return "SourceRequest{" +
            "sourceKey='" + sourceKey + '\'' +
            '}';
  }
}
