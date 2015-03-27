package ru.yandex.se.yasm4u;

import com.spbsu.commons.func.Action;

/**
 * User: solar
 * Date: 16.03.15
 * Time: 15:41
 */
public interface Domain {
  void visitPublic(Action<Ref<?>> visitor);
  Routine[] publicRoutines();

  public interface Controller {
    <T extends Domain> T domain(Class<T> domClass);
    Domain[] domains();
  }
}
