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
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRProcess;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.routines.MRRecord;

import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 13:28
 */
public class MRProcessImpl implements MRProcess {
  private final String name;
  private final String[] goals;
  private final List<MRJoba> jobs = new ArrayList<>();
  private final MRWhiteboardImpl prod;
  private final LocalMREnv cache = new LocalMREnv();
  private final MRWhiteboard test;

  public MRProcessImpl(MREnv env, final String name, final String[] goals) {
    this.name = name;
    this.goals = goals;
    prod = new MRWhiteboardImpl(env, name, System.getenv("USER"));
    test = new MRWhiteboardImpl(cache, name() + "/" + prod.env().name(), System.getenv("USER"), null);
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
    if (prod.env() instanceof YaMREnv) {
      ((YaMREnv)prod.env()).getJarBuilder().setLocalEnv(cache);
    }

    prod.connect(test);
  }

  public String name() {
    return name;
  }

  @Override
  public MRWhiteboard wb() {
    return prod;
  }

  @Override
  public MRState execute() {
    final List<MRJoba> unmergedJobs = unmergeJobs(this.jobs);
    final List<MRJoba> plan = generateExecutionPlan(unmergedJobs);
    for (MRJoba joba : plan) {
      final MRState prevTestState = test.snapshot();
      if (!joba.run(test))
        throw new RuntimeException("MR job failed in test environment: " + joba.toString());
      final MRState nextTestState = test.snapshot();
      for(int i = 0; i < joba.produces().length; i++) {
        final String product = joba.produces()[i];
        if (!nextTestState.available(product))
          throw new RuntimeException("Job " + joba + " failed to produce " + product + " locally!");
      }

      if (!nextTestState.equals(prevTestState) || !checkProductsOlderThenResources(prod, joba.produces(), joba.consumes())) {
        System.out.println("Starting joba " + joba.toString() + " at " + prod.env().name());
        if (!joba.run(prod))
          throw new RuntimeException("MR job failed at production: " + joba.toString());
        final MRState nextProdState = prod.snapshot();
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


  private static class State {
    public final List<MRJoba> plan;
    public double weight = 0;

    private State(List<MRJoba> plan, double weight) {
      this.plan = plan;
      this.weight = weight;
    }

    public State next(MRJoba job) {
      final List<MRJoba> nextPlan = new ArrayList<>(plan);
      nextPlan.add(job);
      return new State(nextPlan, weight + 1);
    }
  }
  /** need to implement Dijkstra's algorithm on state machine in case of several alternative routes
   * @param jobs available moves to make plan */
  private List<MRJoba> generateExecutionPlan(final Collection<MRJoba> jobs) {
    final String[] universe;
    final TObjectIntMap<String> resourceIndices = new TObjectIntHashMap<>();
    final Map<BitSet, State> states = new HashMap<>();
    final TreeSet<BitSet> order = new TreeSet<>(new Comparator<BitSet>() {
      @Override
      public int compare(BitSet o1, BitSet o2) {
        return Double.compare(states.get(o1).weight, states.get(o2).weight);
      }
    });

    { // initialize universe and starting state
      final Set<String> consumes = new HashSet<>();
      final Set<String> produces = new HashSet<>();
      for (final MRJoba job : jobs) {
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
        if (!prod.available(consume))
          throw new IllegalArgumentException("Can not find " + consume + " at production");
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
      states.put(initialState, new State(new ArrayList<MRJoba>(), 0.));
      order.add(initialState);
    }

    BitSet current;
    State best = null;
    double bestScore = Double.POSITIVE_INFINITY;
    while ((current = order.pollFirst()) != null) {
      final State currentState = states.get(current);
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

      for (final MRJoba job : jobs) {
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
        final State nextState = currentState.next(job);
        final State knownState = states.get(next);
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

  private List<MRJoba> unmergeJobs(final List<MRJoba> jobs) {
    final TObjectIntMap<String> sharded = new TObjectIntHashMap<>();
    for (final MRJoba joba : jobs) {
      for (final String resource : joba.produces()) {
        sharded.adjustOrPutValue(resource, 1, 1);
      }
    }

    final Map<String, List<String>> shards = new HashMap<>();
    final List<MRJoba> result = new ArrayList<>();
    for (final MRJoba joba : jobs) {
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
        result.add(new MRJoba() {
          @Override
          public boolean run(MRWhiteboard wb) {
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

  private boolean checkProductsOlderThenResources(MRWhiteboard wb, String[] produces, String[] consumes) {
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

  public void addJob(MRJoba job) {
    jobs.add(job);
  }

  @Override
  public <T> T result() {
    final MRState finalState = execute();
    return finalState.get(goals[0]);
  }
}
