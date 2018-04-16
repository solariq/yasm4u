package com.expleague.yasm4u;

import java.util.List;

/**
 * User: solar
 * Date: 16.03.15
 * Time: 15:41
 */
public interface Domain {
  void publishExecutables(List<Joba> jobs, List<Routine> routines);
  void publishReferenceParsers(Ref.Parser parser, Controller controller);
//  Domain createNSDomain(String ns);

  interface Controller {
    <T extends Domain> T domain(Class<T> domClass);
    Domain[] domains();

    <T, D extends Domain, R extends Ref<? extends T, ? extends D>> R parse(CharSequence seq);

    <T, D extends Domain> T resolve(Ref<T, D> argument);
    <T, D extends Domain> boolean available(Ref<T, D> argument);
  }
}
