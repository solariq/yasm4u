package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRTableShard;
import solar.mr.env.LocalMREnv;
import solar.mr.env.YaMREnv;
import solar.mr.env.YtMREnv;
import solar.mr.proc.CompositeJoba;
import solar.mr.proc.Joba;
import solar.mr.proc.State;
import solar.mr.proc.Whiteboard;
import solar.mr.routines.MRRecord;

import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 13:28
 */
public class CompositeJobaImpl implements CompositeJoba {
  private final String name;
  private final String[] goals;
  private final List<Joba> jobs = new ArrayList<>();
  private final WhiteboardImpl prod;
  private final LocalMREnv cache = new LocalMREnv();
  private final Whiteboard test;

  public CompositeJobaImpl(MREnv env, final String name, final String[] goals) {
    this.name = name;
    this.goals = goals;
    prod = new WhiteboardImpl(env, name, System.getenv("USER"));
    test = new WhiteboardImpl(cache, name() + "/" + prod.env().name(), System.getenv("USER"), null);
    prod.setErrorsHandler(new MRErrorsHandler() {
      @Override
      public void error(final String type, final String cause, MRRecord rec) {
        cache.append(cache.resolve(rec.source), new CharSeqReader(rec.toString() + "\n"));
      }

      @Override
      public void error(final Throwable th, MRRecord rec) {
        cache.append(cache.resolve(rec.source), new CharSeqReader(rec.toString() + "\n"));
      }
    });
    final MREnv prodEnv = prod.env();
    if (prodEnv instanceof YaMREnv) {
      ((YaMREnv)prodEnv).getJarBuilder().setLocalEnv(cache);
    }
    else if (prodEnv instanceof YtMREnv) {
      ((YtMREnv)prodEnv).getJarBuilder().setLocalEnv(cache);
    }

    prod.connect(test);
  }

  public String name() {
    return name;
  }

  @Override
  public boolean run(Whiteboard wb) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] consumes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Whiteboard wb() {
    return prod;
  }

  @Override
  public State execute() {
    final List<Joba> unmergedJobs = unmergeJobs(this.jobs);
    final List<Joba> plan = generateExecutionPlan(unmergedJobs);
    for (Joba joba : plan) {
      final State prevTestState = test.snapshot();
      if (!joba.run(test))
        throw new RuntimeException("MR job failed in test environment: " + joba.toString());
      final State nextTestState = test.snapshot();
      for(int i = 0; i < joba.produces().length; i++) {
        final String product = joba.produces()[i];
        if (!nextTestState.available(product))
          throw new RuntimeException("Job " + joba + " failed to produce " + product + " locally!");
      }

      if (!nextTestState.equals(prevTestState) || !checkProductsOlderThenResources(prod, joba.produces(), joba.consumes())) {
        System.out.println("Starting joba " + joba.toString() + " at " + prod.env().name());
        if (!joba.run(prod))
          throw new RuntimeException("MR job failed at production: " + joba.toString());
        final State nextProdState = prod.snapshot();
        for(int i = 0; i < joba.produces().length; i++) {
          final String product = joba.produces()[i];
          if (!nextProdState.available(product))
            throw new RuntimeException("Job " + joba + " failed to produce " + product + " at production!");
        }
      } else {
        System.out.println("Fast forwarding joba: " + joba.toString());
      }
    }
    return prod.snapshot();
  }

  @Override
  public String[] produces() {
    return Arrays.copyOf(goals, goals.length);
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
   * @param jobs available moves to make plan */
  private List<Joba> generateExecutionPlan(final Collection<Joba> jobs) {
    final String[] universe;
    final TObjectIntMap<String> resourceIndices = new TObjectIntHashMap<>();
    final Map<BitSet, PossibleState> states = new HashMap<>();
    final TreeSet<BitSet> order = new TreeSet<>(new Comparator<BitSet>() {
      @Override
      public int compare(BitSet o1, BitSet o2) {
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
      final Set<String> universeSet = new HashSet<>();
      universeSet.addAll(produces);
      universeSet.addAll(consumes);
      universeSet.addAll(Arrays.asList(goals));
      universe = universeSet.toArray(new String[universeSet.size()]);
      for(int i = 0; i < universe.length; i++) {
        resourceIndices.put(universe[i], i);
      }

      final BitSet initialState = new BitSet(universe.length);
      consumes.removeAll(produces);
      for (final String consume : consumes) {
        // TODO: fix availability concept
        prod.available(consume); // running this function because of it's side effects from lazy tables
        if (!test.available(consume)) {
          if (!prod.processAs(consume, new Processor<MRTableShard>() {
            @Override
            public void process(final MRTableShard prodShard) {
              final CharSeqBuilder builder = new CharSeqBuilder();
              final MRTableShard table = test.get(consume);
              prod.env().sample(prodShard, new Processor<CharSequence>() {
                @Override
                public void process(final CharSequence arg) {
                  builder.append(arg).append('\n');
                }
              });
              test.env().write(table, new CharSeqReader(builder.build()));
            }
          }))
            test.set(consume, prod.get(consume));
        }
        initialState.set(resourceIndices.get(consume), true);
      }
      states.put(initialState, new PossibleState(new ArrayList<Joba>(), 0.));
      order.add(initialState);
    }

    BitSet current;
    PossibleState best = null;
    double bestScore = Double.POSITIVE_INFINITY;
    while ((current = order.pollFirst()) != null) {
      final PossibleState currentState = states.get(current);
      if (bestScore <= currentState.weight)
        continue;
      boolean isFinal = true;
      for(int i = 0; isFinal && i < goals.length; i++) {
        if (!current.get(resourceIndices.get(goals[i])))
          isFinal = false;
      }
      if (isFinal) {
        bestScore = currentState.weight;
        best = currentState;
        continue;
      }

      for (final Joba job : jobs) {
        boolean available = true;
        for (final String resource : job.consumes()) {
          if (!current.get(resourceIndices.get(resource)))
            available = false;
        }
        if (!available)
          continue;
        final BitSet next = (BitSet)current.clone();
        for (final String resource : job.produces()) {
          next.set(resourceIndices.get(resource), true);
        }
        final PossibleState nextState = currentState.next(job);
        final PossibleState knownState = states.get(next);
        if (knownState == null || knownState.weight > nextState.weight) {
          states.put(next, nextState);
          order.add(next);
        }
      }
    }
    if (best == null)
      throw new IllegalArgumentException("Unable to create execution plan");
    return best.plan;
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

  private boolean checkProductsOlderThenResources(Whiteboard wb, String[] produces, String[] consumes) {
    final long[] maxConsumesTime = new long[]{0};
    for(int i = 0; i < consumes.length; i++) {
      wb.processAs(consumes[i], new Processor<MRTableShard>() {
        @Override
        public void process(MRTableShard shard) {
          maxConsumesTime[0] = Math.max(maxConsumesTime[0], shard.metaTS());
        }
      });
    }
    final boolean[] result = new boolean[]{true};
    for(int i = 0; result[0] && i < produces.length; i++) {
      wb.processAs(produces[i], new Processor<MRTableShard>() {
        @Override
        public void process(MRTableShard shard) {
          result[0] &= shard.metaTS() >= maxConsumesTime[0];
        }
      });
    }
    return result[0];
  }

  public void addJob(Joba job) {
    jobs.add(job);
  }

  @Override
  public <T> T result() {
    final State finalState = execute();
    return finalState.get(goals[0]);
  }
}
