package ru.yandex.se.yasm4u.domains.wb;


import ru.yandex.se.yasm4u.Ref;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface Whiteboard extends State {
  State snapshot();

  <T> void set(String var, T data);
  <T> void set(Ref<T> var, T data);
  void remove(String var);

  void wipe();
}
