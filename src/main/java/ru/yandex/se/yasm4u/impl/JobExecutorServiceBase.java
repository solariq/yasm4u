package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.filters.Filter;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.system.RuntimeUtils;
import ru.yandex.se.yasm4u.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User: solar
 * Date: 19.03.15
 * Time: 22:16
 */
public abstract class JobExecutorServiceBase implements JobExecutorService {
  private final Map<Class<? extends Domain>, Domain> domainsCache;
  private final Domain[] domains;
  private final List<ProgressListener> listeners = new ArrayList<>();
  private final List<Routine> routines = new ArrayList<>();
  private final List<Joba> steve = new ArrayList<>();

  public JobExecutorServiceBase(Domain... domains) {
    domainsCache = initDomains(domains);
    this.domains = domains;
    for(int i = 0; i < domains.length; i++) {
      final Domain domain = domains[i];
      final Routine[] routines = domain.publicRoutines();
      for(int j = 0; j < routines.length; j++) {
        addRoutine(routines[j]);
      }
    }
  }

  private Map<Class<? extends Domain>, Domain> initDomains(Domain[] domains) {
    final Map<Class<? extends Domain>, Domain> result = new HashMap<>();
    for(int i = 0; i < domains.length; i++) {
      final Domain domain = domains[i];
      RuntimeUtils.processSupers(domains[i].getClass(), new Filter<Class<?>>() {
        @Override
        public boolean accept(Class<?> aClass) {
          if (Domain.class.isAssignableFrom(aClass))
            //noinspection unchecked
            result.put((Class<? extends Domain>) aClass, domain);
          return false;
        }
      });
    }
    return result;
  }

  @Override
  public <T> Future<T> calculate(Ref<T> goal) {
    final Future<List<?>> calculate = calculate(new Ref[]{goal});
    return new Future<T>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return calculate.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return calculate.isCancelled();
      }

      @Override
      public boolean isDone() {
        return calculate.isDone();
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        return (T)calculate.get().get(0);
      }

      @Override
      public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return (T)calculate.get(timeout, unit).get(0);
      }
    };
  }

  @Override
  public <T extends Domain> T domain(Class<T> domClass) {
    //noinspection unchecked
    return (T)domainsCache.get(domClass);
  }

  @Override
  public Domain[] domains() {
    return domains;
  }

  @Override
  public void addRoutine(Routine routine) {
    routines.add(routine);
  }

  @Override
  public void addJoba(Joba joba) {
    steve.add(joba);
  }

  @Override
  public void addListener(ProgressListener listener) {
    listeners.add(listener);
  }

  protected Routine[] routines() {
    return routines.toArray(new Routine[routines.size()]);
  }

  protected Joba[] jobs() {
    return steve.toArray(new Joba[steve.size()]);
  }

  protected void invoke(Action<ProgressListener> action) {
    for(int i = 0; i < listeners.size(); i++) {
      action.invoke(listeners.get(i));
    }
  }
}
