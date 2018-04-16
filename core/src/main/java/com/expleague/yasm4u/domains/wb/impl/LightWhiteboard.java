package com.expleague.yasm4u.domains.wb.impl;

import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import org.jetbrains.annotations.Nullable;
import com.expleague.yasm4u.domains.wb.StateRef;

import java.util.List;
import java.util.Set;

/**
 * User: solar
 * Date: 24.04.15
 * Time: 17:16
 */
public class LightWhiteboard implements Whiteboard {
  @Override
  public void remove(StateRef var) {

  }

  @Override
  public State snapshot() {
    return null;
  }

  @Override
  public <T> void set(String var, T data) {

  }

  @Override
  public <T> void set(StateRef uri, T data) {

  }

  @Override
  public void remove(String var) {

  }

  @Override
  public void wipe() {

  }

  @Nullable
  @Override
  public <T> T get(String uri) {
    return null;
  }

  @Nullable
  @Override
  public <T> T get(StateRef<T> name) {
    return null;
  }

  @Override
  public boolean available(String... consumes) {
    return false;
  }

  @Override
  public boolean available(StateRef... consumes) {
    return false;
  }

  @Override
  public Set<? extends StateRef> keys() {
    return null;
  }

  @Override
  public <T extends StateRef> T parseRef(CharSequence from) {
    return null;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {

  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Domain.Controller controller) {

  }
}
