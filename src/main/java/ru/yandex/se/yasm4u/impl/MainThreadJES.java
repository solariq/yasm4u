package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.util.CompleteFuture;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * User: solar
 * Date: 19.03.15
 * Time: 22:40
 */
public class MainThreadJES extends JobExecutorServiceBase {
  private final boolean safe;

  public MainThreadJES(boolean safe, Domain... domains) {
    super(domains);
    this.safe = safe;
  }

  public MainThreadJES(Domain... domains) {
    this(true, domains);
  }

  @Override
  public Future<List<?>> calculate(Ref<?>... goal) {
    final Planner planner = new Planner(new Ref[0], routines(), jobs());
    final Joba[] plan = planner.build(this, goal);
    for(int i = 0; i < plan.length; i++) {
      final Joba joba = plan[i];
      if (safe) {
        for(int j = 0; j < plan[i].consumes().length; j++) {
          final Ref<?> ref = plan[i].consumes()[j];
          if (!ref.available(this))
            throw new RuntimeException("Resource " + ref + " needed for " + joba + " is missing at " + domain(ref.domainType()));
        }
      }
      invoke(new Action<ProgressListener>() {
        @Override
        public void invoke(ProgressListener listener) {
          listener.jobStart(joba);
        }
      });
      try {
        plan[i].run();
      }
      catch(final Exception e) {
        invoke(new Action<ProgressListener>() {
          @Override
          public void invoke(ProgressListener listener) {
            listener.jobException(e, joba);
          }
        });
      }
      if (safe) {
        for(int j = 0; j < plan[i].produces().length; j++) {
          final Ref<?> ref = plan[i].produces()[j];
          if (!ref.available(this))
            throw new RuntimeException("Complete joba " + joba + " has not produced expected resource " + ref);
        }
      }
      invoke(new Action<ProgressListener>() {
        @Override
        public void invoke(ProgressListener listener) {
          listener.jobFinish(joba);
        }
      });
    }
    final List result = new ArrayList<>();
    for(int i = 0; i < goal.length; i++) {
      //noinspection unchecked
      result.add(goal[i].resolve(this));

    }
    return new CompleteFuture<List<?>>(result);
  }

}
