package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
public class StartResponseRef implements Ref<Void,UserHttpCommunicationDomain> {
  private int partsCount;

  public StartResponseRef(int partsCount) {
    this.partsCount = partsCount;
  }

  public int partsCount() {
    return partsCount;
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<Void> type() {
    return Void.class;
  }

  @Override
  public Class<UserHttpCommunicationDomain> domainType() {
    return UserHttpCommunicationDomain.class;
  }

  @Override
  public Void resolve(UserHttpCommunicationDomain controller) {
    return null;
  }

  @Override
  public boolean available(UserHttpCommunicationDomain controller) {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final StartResponseRef that = (StartResponseRef) o;
    return partsCount == that.partsCount;
  }

  @Override
  public int hashCode() {
    return partsCount;
  }
}
