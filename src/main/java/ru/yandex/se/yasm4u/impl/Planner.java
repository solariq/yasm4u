package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.func.Evaluator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.yandex.se.yasm4u.*;

import java.util.*;

/**
 * User: solar
 * Date: 17.03.15
 * Time: 16:53
 */
public class Planner {
  private final Ref[] initial;
  private final Routine[] routines;
  private final Joba[] jobs;

  private final Evaluator<Joba> estimator = new Evaluator<Joba>() {
    @Override
    public double value(Joba joba) {
      return 1.;
    }
  };

  public Planner(Ref[] initial, Routine[] routines, Joba[] jobs) {
    this.initial = initial;
    this.routines = routines;
    this.jobs = jobs;
  }

  @NotNull
  public Joba[] build(JobExecutorService jes, Ref<?>... goals) {
    final Set<Ref<?>> initialState;
    { // initialize universe and starting state
      Set<Ref<?>> consumes = new HashSet<>();
      final Set<Ref<?>> produces = new HashSet<>();
      for (Routine routine : routines) {
        for (final Joba job : routine.buildVariants(initial, jes)) {
          consumes.addAll(Arrays.asList(job.consumes()));
          produces.addAll(Arrays.asList(job.produces()));
        }
      }

      for (final Joba job : jobs) {
        consumes.addAll(Arrays.asList(job.consumes()));
        produces.addAll(Arrays.asList(job.produces()));
      }

      consumes.removeAll(produces);
      int consumesSize;
      do {
        consumesSize = consumes.size();
        final Iterator<Ref<?>> it = consumes.iterator();
        Set<Ref<?>> next = new HashSet<>(consumes);
        while (it.hasNext()) {
          final Ref<?> res = it.next();
          next.remove(res);
          if (buildOptimalPath(jes, next, res) == null)
            next.add(res);
        }
        consumes = next;
      }
      while(consumesSize > consumes.size());

      initialState = new HashSet<>(consumes);
    }


    final PossibleState best = buildOptimalPath(jes, initialState, goals);
    if (best == null)
      throw new IllegalStateException("Unable to create plan to execute");
    return best.plan.toArray(new Joba[best.plan.size()]);
  }

  @Nullable
  protected PossibleState buildOptimalPath(JobExecutorService jes, Set<Ref<?>> initialState, Ref<?>... goals) {
    final Map<Set<Ref<?>>, PossibleState> states = new HashMap<>();
    final TreeSet<Set<Ref<?>>> order = new TreeSet<>(new Comparator<Set<Ref<?>>>() {
      @Override
      public int compare(Set<Ref<?>> o1, Set<Ref<?>> o2) {
        final int compare = Double.compare(states.get(o1).weight, states.get(o2).weight);
        if (compare == 0)
          return Integer.compare(o1.hashCode(), o2.hashCode());
        return compare;
      }
    });

    states.put(initialState, new PossibleState(new ArrayList<Joba>(), 0.));
    order.add(initialState);

    Set<Ref<?>> current;
    PossibleState best = null;
    double bestScore = Double.POSITIVE_INFINITY;
    while ((current = order.pollFirst()) != null) {
      final PossibleState currentState = states.get(current);
      if (bestScore <= currentState.weight)
        continue;
      boolean isFinal = true;
      for(int i = 0; isFinal && i < goals.length; i++) {
        if (!current.contains(goals[i]))
          isFinal = false;
      }
      if (isFinal) {
        bestScore = currentState.weight;
        best = currentState;
        continue;
      }

      final List<Joba> currentJobs = new ArrayList<>(Arrays.asList(jobs));
      final Ref[] state = current.toArray(new Ref[current.size()]);
      for (final Routine routine : routines) {
        currentJobs.addAll(Arrays.asList(routine.buildVariants(state, jes)));
      }

      for (final Joba job : currentJobs) {
        if (!current.containsAll(Arrays.asList(job.consumes())))
          continue;
        final Set<Ref<?>> next = new HashSet<>(current);
        next.addAll(Arrays.asList(job.produces()));
        final PossibleState nextState = currentState.next(job);
        final PossibleState knownState = states.get(next);
        if (knownState == null || knownState.weight > nextState.weight) {
          states.put(next, nextState);
          order.add(next);
        }
      }
    }
    return best;
  }

  private class PossibleState {
    public final List<Joba> plan;
    public double weight = 0;

    private PossibleState(List<Joba> plan, double weight) {
      this.plan = plan;
      this.weight = weight;
    }

    public PossibleState next(Joba job) {
      final List<Joba> nextPlan = new ArrayList<>(plan);
      nextPlan.add(job);
      return new PossibleState(nextPlan, weight + estimator.value(job));
    }
  }
}
