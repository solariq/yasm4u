package ru.yandex.se.yasm4u;

/**
 * User: solar
 * Date: 16.03.15
 * Time: 15:41
 */
public interface Domain {
  void init(JobExecutorService jes);
//  Domain createNSDomain(String ns);

  public interface Controller {
    <T extends Domain> T domain(Class<T> domClass);
    Domain[] domains();
  }
}
