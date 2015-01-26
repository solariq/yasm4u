package solar.mr.proc.impl;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.jetbrains.annotations.Nullable;
import solar.mr.proc.Joba;
import solar.mr.proc.Routine;
import solar.mr.proc.Whiteboard;

import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 13:28
 */
public class CompositeJobaBuilder {
  private final String name;
  private final String[] goals;
  private final List<Joba> jobs = new ArrayList<>();
  private final List<Routine> routines = new ArrayList<>();

  public CompositeJobaBuilder(final String name, final String[] goals) {
    this.name = name;
    this.goals = goals;
  }

  public String name() {
    return name;
  }

  public void addJob(Joba job) {
    jobs.add(job);
  }

  public void addRoutine(Routine routine) {
    routines.add(routine);
  }

  @Nullable
  public Joba build() {
    final String[] produces = Arrays.copyOf(goals, goals.length);
    final List<Joba> unmergedJobs = unmergeJobs(this.jobs);
    final List<String> consumesLst = new ArrayList<>();
    final List<Joba> plan = generateExecutionPlan(unmergedJobs, routines, consumesLst, goals);
    if (plan == null)
      return null;
    final String[] consumes = consumesLst.toArray(new String[consumesLst.size()]);
    return new Joba() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public boolean run(Whiteboard wb) {
        for (Joba joba : plan) {
          if (!joba.run(wb))
            throw new RuntimeException("MR job failed: " + joba.toString());
        }
        return true;
      }

      @Override
      public String[] consumes() {
        return consumes;
      }

      @Override
      public String[] produces() {
        return produces;
      }
    };
  }

  private static class PossibleState {
    public final List<Joba> plan;
    public double weight = 0;

    private PossibleState(List<Joba> plan, double weight) {
      this.plan = plan;
      this.weight = weight;
    }

    public PossibleState next(Joba job) {
      final List<Joba> nextPlan = new ArrayList<>(plan);
      nextPlan.add(job);

      return new PossibleState(nextPlan, weight + 1);
    }
  }
  /** need to implement Dijkstra's algorithm on state machine in case of several alternative routes
   * @param jobs available moves to make plan
   */
   @Nullable
   private static List<Joba> generateExecutionPlan(final Collection<Joba> jobs, final List<Routine> routines, final Collection<String> consumesLst, String[] goals) {
    final Map<Set<String>, PossibleState> states = new HashMap<>();
    final TreeSet<Set<String>> order = new TreeSet<>(new Comparator<Set<String>>() {
      @Override
      public int compare(Set<String> o1, Set<String> o2) {
        return Double.compare(states.get(o1).weight, states.get(o2).weight);
      }
    });

    { // initialize universe and starting state
      final Set<String> consumes = new HashSet<>();
      final Set<String> produces = new HashSet<>();
      for (final Joba job : jobs) {
        consumes.addAll(Arrays.asList(job.consumes()));
        produces.addAll(Arrays.asList(job.produces()));
      }

      consumes.removeAll(produces);
      final Set<String> initialState = new HashSet<>(consumes);
      states.put(initialState, new PossibleState(new ArrayList<Joba>(), 0.));
      order.add(initialState);
    }

    Set<String> current;
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

      for (final Joba job : jobs) {
        if (!current.containsAll(Arrays.asList(job.consumes())))
          continue;
        final Set<String> next = new HashSet<>(current);
        next.addAll(Arrays.asList(job.produces()));
        final PossibleState nextState = currentState.next(job);
        final PossibleState knownState = states.get(next);
        if (knownState == null || knownState.weight > nextState.weight) {
          states.put(next, nextState);
          order.add(next);
        }
      }
      for (final Routine routine : routines) {
        @SuppressWarnings("unchecked")
        final List<String>[] variants = (List<String>[])new List[routine.dim()];
        for (final String resource : current) {
          for (int i = 0; i < routine.dim(); i++) {
            if (routine.isRelevant(resource, i))
              variants[i].add(resource);
          }
        }

        int variantsCount = 1;
        for(int i = 0; i < variants.length; i++) {
          variantsCount *= variants[i].size();
        }

        for (int v = 0; v < variantsCount; v++) {
          final String[] variant = new String[routine.dim()];
          int currentVar = v;
          for (int i = 0; i < variants.length; i++) {
            variant[i] = variants[i].get(currentVar % variants[i].size());
            currentVar /= variants[i].size();
          }
          final Joba job = routine.build(variant);
          final Set<String> next = new HashSet<>(current);
          next.addAll(Arrays.asList(job.produces()));
          final PossibleState nextState = currentState.next(job);
          final PossibleState knownState = states.get(next);
          if (knownState == null || knownState.weight > nextState.weight) {
            states.put(next, nextState);
            order.add(next);
          }
        }
      }
    }
    return best != null ? best.plan : null;
  }

  private List<Joba> unmergeJobs(final List<Joba> jobs) {
    final TObjectIntMap<String> sharded = new TObjectIntHashMap<>();
    for (final Joba joba : jobs) {
      for (final String resource : joba.produces()) {
        sharded.adjustOrPutValue(resource, 1, 1);
      }
    }

    final Map<String, List<String>> shards = new HashMap<>();
    final List<Joba> result = new ArrayList<>();
    for (final Joba joba : jobs) {
      final String[] outputs = new String[joba.produces().length];
      for(int i = 0; i < outputs.length; i++) {
        final String resourceName = joba.produces()[i];
        if (sharded.get(resourceName) > 1) {
          List<String> shards4resource = shards.get(resourceName);
          if (shards4resource == null)
            shards.put(resourceName, shards4resource = new ArrayList<>());
          outputs[i] = "temp:" + resourceName + "-" + shards4resource.size();
          shards4resource.add(outputs[i]);
        }
        else outputs[i] = resourceName;
      }
      if (!Arrays.equals(outputs, joba.produces())) {
        result.add(new Joba() {
          @Override
          public String name() {
            return toString();
          }

          @Override
          public boolean run(Whiteboard wb) {
            synchronized (joba) {
              final Object[] resolved = new Object[outputs.length];
              for(int i = 0; i < outputs.length; i++) {
                resolved[i] = wb.get(joba.produces()[i]);
              }
              try {
                for(int i = 0; i < outputs.length; i++) {
                  wb.set(joba.produces()[i], wb.get(outputs[i]));
                }
                return joba.run(wb);
              }
              finally {
                for(int i = 0; i < outputs.length; i++) {
                  wb.set(joba.produces()[i], resolved[i]);
                }
              }
            }
          }

          @Override
          public String[] consumes() {
            return joba.consumes();
          }

          @Override
          public String[] produces() {
            return outputs;
          }

          @Override
          public String toString() {
            return "SplitAdapter for " + joba.toString();
          }
        });
      }
      else result.add(joba);
    }
    for (final Map.Entry<String, List<String>> entry : shards.entrySet()) {
      final List<String> nextShards = entry.getValue();
      result.add(new MergeJoba(nextShards.toArray(new String[nextShards.size()]), entry.getKey()));
    }
    return result;
  }
}
