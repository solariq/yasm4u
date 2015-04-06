package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.util.CompleteFuture;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

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
  @SuppressWarnings("FieldCanBeLocal")
  private ProgressListener errorsPrinter;

  public MainThreadJES(boolean safe, Domain... domains) {
    super(domains);
    this.safe = safe;
    errorsPrinter = new ProgressListener() {
      @Override
      public void jobStart(Joba joba) {
        System.out.println("YASM4U: " + joba.toString() + " started");
      }

      @Override
      public void jobFinish(Joba joba) {
        System.out.println("YASM4U: " + joba.toString() + " finished");
      }

      @Override
      public void jobException(Exception e, Joba joba) {
        throw new RuntimeException(e);
      }
    };
    addListener(errorsPrinter);
  }

  public MainThreadJES(Domain... domains) {
    this(true, domains);
  }

  @Override
  public Future<List<?>> calculate(Ref... goal) {
    final Planner planner = new Planner(new Ref[0], routines(), jobs());
    final Joba[] plan = planner.build(this, goal);
    for(int i = 0; i < plan.length; i++) {
      final Joba joba = plan[i];
      if (safe) {
        for(int j = 0; j < plan[i].consumes().length; j++) {
          final Ref ref = plan[i].consumes()[j];
          if (!available(ref))
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
          final Ref ref = plan[i].produces()[j];
          if (!available(ref))
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
      result.add(resolve(goal[i]));

    }
    return new CompleteFuture<List<?>>(result);
  }
}
