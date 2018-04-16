package com.expleague.yasm4u.domains.wb;


/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface Whiteboard extends State {
  void remove(StateRef var);

  State snapshot();

  <T> void set(String var, T data);

  <T> void set(StateRef uri, T data);

  void remove(String var);

  void wipe();
}
