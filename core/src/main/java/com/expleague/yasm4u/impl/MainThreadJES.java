package com.expleague.yasm4u.impl;

import com.expleague.commons.util.CompleteFuture;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
  public Future<List<?>> calculate(Set<Ref> from, Ref... goal) {
    final Planner planner = new Planner(from.toArray(new Ref[from.size()]), routines(), jobs());
    final Joba[] plan = planner.build(this, goal);
    for (final Joba joba : plan) {
      if (safe) {
        for (int j = 0; j < joba.consumes().length; j++) {
          final Ref ref = joba.consumes()[j];
          if (!available(ref))
            throw new RuntimeException("Resource " + ref + " needed for " + joba + " is missing at " + domain(ref.domainType()));
        }
      }
      invoke(listener -> listener.jobStart(joba));
      try {
        joba.run();
      }
      catch (final Exception e) {
        invoke(listener -> listener.jobException(e, joba));
      }
      if (safe) {
        for (int j = 0; j < joba.produces().length; j++) {
          final Ref ref = joba.produces()[j];
          if (!available(ref))
            throw new RuntimeException("Complete joba " + joba + " has not produced expected resource " + ref);
        }
      }
      invoke(listener -> listener.jobFinish(joba));
    }
    final List result = new ArrayList<>();
    for(int i = 0; i < goal.length; i++) {
      //noinspection unchecked
      result.add(resolve(goal[i]));

    }
    return new CompleteFuture<List<?>>(result);
  }
}
