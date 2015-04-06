package ru.yandex.se.yasm4u.domains.wb.impl;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;

import java.util.Set;

/**
 * User: solar
 * Date: 30.03.15
 * Time: 19:47
 */
public class PublisherJoba implements Joba {
  private final State state;

  public PublisherJoba(State state) {
    this.state = state;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[0];
  }

  @Override
  public Ref[] produces() {
    final Set<? extends StateRef> keys = state.keys();
    return state.keys().toArray(new Ref[keys.size()]);
  }

  @Override
  public void run() {
  }

  @Override
  public String toString() {
    return "WB publisher";
  }
}
