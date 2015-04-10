package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.filters.Filter;
import com.spbsu.commons.func.Evaluator;
import com.spbsu.commons.util.ArrayTools;
import org.jetbrains.annotations.NotNull;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

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
  public Joba[] build(JobExecutorService jes, Ref... goals) {
    final Set<Ref> initialState;
    { // initialize universe and starting state
      Set<Ref> consumes = new HashSet<>();
      final Set<Ref> produces = new HashSet<>();
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
      final Set<Joba> possibleJobas = new HashSet<>();
      final Set<Ref> possibleResources = new HashSet<>();

      do {
        consumesSize = consumes.size();
        final Iterator<Ref> it = consumes.iterator();
        Set<Ref> next = new HashSet<>(consumes);
        while (it.hasNext()) {
          final Ref res = it.next();
          next.remove(res);
          if (!forwardPath(jes, next, possibleJobas, possibleResources, res)) {
            forwardPath(jes, next, possibleJobas, possibleResources, res);
            next.add(res);
          }
        }
        consumes = next;
      }
      while(consumesSize > consumes.size());

      initialState = new HashSet<>(consumes);
    }


    final Set<Joba> possibleJobas = new HashSet<>();
    final Set<Ref> possibleResources = new HashSet<>();

    forwardPath(jes, initialState, possibleJobas, possibleResources, goals);
    final List<Joba> best = backwardPath(goals, possibleJobas, initialState);
    return best.toArray(new Joba[best.size()]);
  }

  private List<Joba> backwardPath(Ref[] goals, Set<Joba> possibleJobas, Set<Ref> initialState) {
    final Map<Set<Ref>, PossibleState> states = new HashMap<>();
    final TreeSet<Set<Ref>> order = new TreeSet<>(new Comparator<Set<Ref>>() {
      @Override
      public int compare(Set<Ref> o1, Set<Ref> o2) {
        final int cmp1 = Double.compare(states.get(o1).weight, states.get(o2).weight);
        if (cmp1 == 0) {
          final int cmp2 = Integer.compare(o1.hashCode(), o2.hashCode());
          return cmp2 == 0 && !o1.equals(o2)? 1 : cmp2;
        }
        return cmp1;
      }
    });

    final HashSet<Ref> goalsSet = new HashSet<>(Arrays.asList(goals));
    states.put(goalsSet, new PossibleState(new ArrayList<Joba>(), 0.));
    order.add(goalsSet);

    Set<Ref> current;
    PossibleState best = null;
    double bestScore = Double.POSITIVE_INFINITY;
    while ((current = order.pollFirst()) != null) {
      final PossibleState currentState = states.get(current);
      if (bestScore <= currentState.weight)
        continue;
      if (initialState.containsAll(current)) {
        bestScore = currentState.weight;
        best = currentState;
        continue;
      }

      for (final Joba job : possibleJobas) {
        final Set<Ref> finalCurrent = current;
        if (!ArrayTools.or(job.produces(), new Filter<Ref>() {
          @Override
          public boolean accept(Ref ref) {
            return finalCurrent.contains(ref);
          }
        }))
          continue;
        final Set<Ref> next = new HashSet<>(current);
        next.addAll(Arrays.asList(job.consumes()));
        next.removeAll(Arrays.asList(job.produces()));
        final PossibleState nextState = currentState.next(job);
        final PossibleState knownState = states.get(next);
        if (knownState == null || knownState.weight > nextState.weight) {
          states.put(next, nextState);
          order.remove(next);
          order.add(next);
        }
      }
    }
    if (best == null)
      throw new RuntimeException("Unable to create feasible plan! This should not happen.");
    Collections.reverse(best.plan);
    return best.plan;
  }

  private boolean forwardPath(JobExecutorService jes, Set<Ref> initialState, Set<Joba> possibleJobas, Set<Ref> possibleResources, Ref... goals) {
    possibleJobas.clear();
    possibleResources.clear();
    possibleJobas.addAll(Arrays.asList(jobs));
    possibleResources.addAll(initialState);
    final Set<Ref> goalsSet = new HashSet<>(Arrays.asList(goals));
    final List<Joba> jobs = new ArrayList<>(Arrays.asList(this.jobs));
    while(!possibleResources.containsAll(goalsSet)) {
      final Ref[] state = possibleResources.toArray(new Ref[possibleResources.size()]);
      for (final Routine routine : this.routines) {
        final List<Joba> routineGenerated = Arrays.asList(routine.buildVariants(state, jes));
        jobs.addAll(routineGenerated);
      }
      final Iterator<Joba> jobsIt = jobs.iterator();
      while (jobsIt.hasNext()) {
        final Joba joba = jobsIt.next();
        if (possibleResources.containsAll(Arrays.asList(joba.consumes()))) {
          possibleResources.addAll(Arrays.asList(joba.produces()));
          possibleJobas.add(joba);
          jobsIt.remove();
        }
      }
      if (possibleResources.size() == state.length && !possibleResources.containsAll(goalsSet)) {
        goalsSet.removeAll(possibleResources);
        return false;
      }
    }
    return true;
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
