package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.filters.Filter;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.system.RuntimeUtils;
import org.jetbrains.annotations.NotNull;
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
  private RefParserImpl refParser = new RefParserImpl();

  public JobExecutorServiceBase(Domain... domains) {
    this.domainsCache = createDomainsCache(domains);
    this.domains = domains;
    for(int i = 0; i < domains.length; i++) {
      domains[i].publishExecutables(steve, routines);
      domains[i].publishReferenceParsers(refParser, this);
    }
  }

  private Map<Class<? extends Domain>, Domain> createDomainsCache(Domain[] domains) {
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
  public <T> Future<T> calculate(Ref<T, ?> goal) {
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
        //noinspection unchecked
        return (T)calculate.get().get(0);
      }

      @Override
      public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        //noinspection unchecked
        return (T)calculate.get(timeout, unit).get(0);
      }
    };
  }

  @Override
  public <T, D extends Domain, R extends Ref<? extends T, ? extends D>> R parse(CharSequence seq) {
    //noinspection unchecked
    return (R) refParser.convert(seq);
  }

  @Override
  public <T extends Domain> T domain(Class<T> domClass) {
    //noinspection unchecked
    return (T)domainsCache.get(domClass);
  }

  @Override
  public <T, D extends Domain> T resolve(Ref<T, D> argument) {
    return argument.resolve(domain(argument.domainType()));
  }

  @Override
  public <T, D extends Domain> boolean available(Ref<T, D> argument) {
    return argument.available(domain(argument.domainType()));
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
