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
    final List<MRJoba> unmergedJobs = unmergeJobs(this.jobs, prod);
    final List<MRJoba> plan = generateExecutionPlan(unmergedJobs);
    for (MRJoba joba : plan) {
      runJoba(joba);
    }
    return prod.snapshot();
  }

  /** need to implement Dijkstra's algorithm on state machine in case of several alternative routes
   * @param jobs*/
  private List<MRJoba> generateExecutionPlan(final List<MRJoba> jobs) {
    final Deque<MRJoba> result = new ArrayDeque<>();
    final List<String> unresolved = new ArrayList<>();
    final Set<String> resolved = new HashSet<>();
    unresolved.addAll(Arrays.asList(goals));
    while (!unresolved.isEmpty()) {
      final Iterator<String> iterator = unresolved.iterator();
      final String resource2resolve = iterator.next();

      if (!test.depends(resource2resolve).isEmpty()) {
        for (final String deps : test.depends(resource2resolve)) {
          test.set(deps, prod.resolve(deps));
        }
        unresolved.add(0, prod.resolveName(resource2resolve));
      }
      else {
        final List<MRJoba> producers = new ArrayList<>(jobs.size());
        for (final MRJoba job : jobs) {
          if (ArrayTools.indexOf(resource2resolve, job.produces(prod)) >= 0)
            producers.add(job);
        }

        if (producers.isEmpty()) { // need to sample or copy
          if (!prod.check(resource2resolve))
            throw new RuntimeException("Resource is not available at production: " + resource2resolve + " as " + prod.resolve(resource2resolve));
          if (!test.check(resource2resolve)) {
            final Object resolution = prod.resolve(resource2resolve);
            if (resolution instanceof MRTableShard) {
              final CharSeqBuilder builder = new CharSeqBuilder();
              final MRTableShard table = test.resolve(resource2resolve);
              final MRTableShard prodShard = (MRTableShard) resolution;
              prod.env().sample(prodShard, new Processor<CharSequence>() {
                @Override
                public void process(final CharSequence arg) {
                  builder.append(arg).append('\n');
                }
              });
              test.env().write(table, new CharSeqReader(builder));
            } else
              test.set(resource2resolve, resolution);
          }
        } else {
          final MRJoba joba = producers.get(0);
          result.push(joba); // multiple ways of producing the same resource is not supported yet
          for (final String product : joba.produces(prod)) {
            unresolved.remove(product);
            resolved.add(product);
          }
          for (final String resource : joba.consumes(prod)) {
            if (!resolved.contains(resource)) {
              unresolved.add(resource);
            }
          }
        }
      }

      resolved.add(resource2resolve);
      unresolved.remove(resource2resolve);
    }
    return Arrays.asList(result.toArray(new MRJoba[result.size()]));
  }

  public String[] goals(MRWhiteboard wb) {
    final String[] result = new String[goals.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = wb.resolveName(goals[i]);
    }
    return result;
  }

  private List<MRJoba> unmergeJobs(final List<MRJoba> jobs, MRWhiteboardImpl wb) {
    final TObjectIntMap<String> sharded = new TObjectIntHashMap<>();
    for (final MRJoba joba : jobs) {
      for (final String resource : joba.produces(wb)) {
        sharded.adjustOrPutValue(resource, 1, 1);
      }
    }

    final Map<String, List<String>> shards = new HashMap<>();
    final List<MRJoba> result = new ArrayList<>();
    for (final MRJoba joba : jobs) {
      final String[] outputs = new String[joba.produces(wb).length];
      for(int i = 0; i < outputs.length; i++) {
        final String resourceName = joba.produces(wb)[i];
        if (sharded.get(resourceName) > 1) {
          List<String> shards4resource = shards.get(resourceName);
          if (shards4resource == null)
            shards.put(resourceName, shards4resource = new ArrayList<>());
          outputs[i] = "temp:" + wb.resolveName(resourceName) + "-" + shards4resource.size();
          shards4resource.add(outputs[i]);
        }
        else outputs[i] = resourceName;
      }
      if (!Arrays.equals(outputs, joba.produces(wb))) {
        result.add(new MRJoba() {
          @Override
          public boolean run(MRWhiteboard wb) {
            synchronized (joba) {
              final Object[] resolved = new Object[outputs.length];
              for(int i = 0; i < outputs.length; i++) {
                resolved[i] = wb.resolve(joba.produces(wb)[i]);
              }
              try {
                for(int i = 0; i < outputs.length; i++) {
                  wb.set(joba.produces(wb)[i], wb.resolve(outputs[i]));
                }
                return joba.run(wb);
              }
              finally {
                for(int i = 0; i < outputs.length; i++) {
                  wb.set(joba.produces(wb)[i], resolved[i]);
                }
              }
            }
          }

          @Override
          public String[] consumes(MRWhiteboard wb) {
            return joba.consumes(wb);
          }

          @Override
          public String[] produces(MRWhiteboard wb) {
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

  private MRState runJoba(MRJoba mrJoba) {
    boolean needToRunProd = !prod.check(mrJoba.produces(prod));

    final Map<String, String> crcs = new HashMap<>();
    for (final String productName : mrJoba.produces(test)) {
      final Object product = test.resolve(productName);
      if (product instanceof MRTableShard) {
        final MRTableShard productTable = (MRTableShard) product;
        crcs.put(productName, productTable.isAvailable() ? productTable.crc() : "");
      }
    }
    if (!mrJoba.run(test))
      throw new RuntimeException("MR job failed in test environment: " + mrJoba.toString());

    final MRState next = test.snapshot();

    // checking what have been produced
    for (final String productName : mrJoba.produces(test)) {
      if (!next.available(productName))
        throw new RuntimeException("MR job " + mrJoba.toString() + " failed to produce resource " + productName);
      final Object product = test.refresh(productName);
      if (product instanceof MRTableShard) {
        needToRunProd |= !((MRTableShard) product).crc().equals(crcs.get(productName));
      }
    }
    if (needToRunProd) {
      System.out.println("Starting joba " + mrJoba.toString() + " at " + prod.env().name());
      if (!mrJoba.run(prod))
        throw new RuntimeException("MR job failed at production: " + mrJoba.toString());
    } else {
      System.out.println("Fast forwarding joba: " + mrJoba.toString());
    }
    return next;
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
