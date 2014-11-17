package solar.mr.proc.impl;

import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.util.ArrayTools;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.routines.MRRecord;
import solar.mr.env.LocalMREnv;
import solar.mr.env.YaMREnv;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRProcess;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.MRTableShard;

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
    final ArrayDeque<MRJoba> plan = new ArrayDeque<>();
    if (!generateExecutionPlan(unmergedJobs, new LinkedHashSet<>(Arrays.asList(goals)), plan))
      throw new RuntimeException("Unable to create execution plan");
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

  /** need to implement Dijkstra's algorithm on state machine in case of several alternative routes
   * @param jobs available moves to make plan */
  private boolean generateExecutionPlan(final Collection<MRJoba> jobs, final Set<String> state, final Deque<MRJoba> result) {
    for (final String trying2resolve : state) {
      final List<MRJoba> producers = new ArrayList<>(jobs.size());
      for (final MRJoba job : jobs) {
        if (ArrayTools.indexOf(trying2resolve, job.produces()) >= 0)
          producers.add(job);
      }

      if (producers.isEmpty()) { // need to sample or copy
        if (!prod.available(trying2resolve))
          return false;
        if (!test.available(trying2resolve)) {
          if (!prod.processAs(trying2resolve, new Processor<MRTableShard>() {
            @Override
            public void process(final MRTableShard prodShard) {
              final CharSeqBuilder builder = new CharSeqBuilder();
              final MRTableShard table = test.get(trying2resolve);
              prod.env().sample(prodShard, new Processor<CharSequence>() {
                @Override
                public void process(final CharSequence arg) {
                  builder.append(arg).append('\n');
                }
              });
              test.env().write(table, new CharSeqReader(builder));
            }
          }))
            test.set(trying2resolve, prod.get(trying2resolve));
        }
        state.remove(trying2resolve);
      }
      else {
        final MRJoba joba = producers.get(0);
        result.push(joba); // multiple ways of producing the same resource is not supported yet
        jobs.removeAll(producers);
        final Set<String> next = new HashSet<>(state);
        next.removeAll(Arrays.asList(joba.produces()));
        next.addAll(Arrays.asList(joba.consumes()));
        if (generateExecutionPlan(jobs, next, result))
          return true;
        jobs.addAll(producers);
      }
    }
    return state.isEmpty();
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
