package solar.mr.proc.impl;

import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRTable;
import solar.mr.env.LocalMREnv;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRProcess;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 13:28
 */
public class MRProcessImpl implements MRProcess {
  private final String name;
  private final String goal;
  private final List<MRJoba> jobs = new ArrayList<>();
  private final MRWhiteboard prod;

  public MRProcessImpl(MREnv env, final String name, final String goal) {
    this.name = name;
    this.goal = goal;
    prod = new MRWhiteboardImpl(env, name, System.getenv("USER"));
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
    final LocalMREnv cache = new LocalMREnv();
    final MRWhiteboard test = new MRWhiteboardImpl(cache, name() + "/" + prod.env().name(), System.getenv("USER"), new MRErrorsHandler() {
      @Override
      public void error(final String type, final String cause, final String table, final CharSequence record) {
        cache.append(cache.resolve(table), new CharSeqReader(record));
      }
      @Override
      public void error(final Throwable th, final String table, final CharSequence record) {
        cache.append(cache.resolve(table), new CharSeqReader(record));
      }
    });
    {
      final MRState state = prod.slice();
      for (final String resourceName : state.available()) {
        final Object resource = state.get(resourceName);
        // TODO: this logic is not enough here, we need to avoid setting stuff that could be produced. For this we need analyse dependencies graph
        if (!(resource instanceof MRTableShard))
          test.set(resourceName, resource);
      }
    }
    // at start banning all producible resources
    final Set<String> bannedResources = new HashSet<>();
    final Set<String> neededResources = new HashSet<>();
    for (int j = 0; j < jobs.size(); j++) {
      bannedResources.addAll(Arrays.asList(jobs.get(j).produces()));
      neededResources.addAll(Arrays.asList(jobs.get(j).consumes()));
    }
    MRState current = test.slice();
    neededResources.removeAll(bannedResources);
    for (String resourceName : neededResources) {
      if (current.available(resourceName))
        continue;
      final Object resource = test.resolve(resourceName);
      if (resource instanceof MRTableShard) {
        final CharSeqBuilder builder = new CharSeqBuilder();
        final MRTableShard table = (MRTableShard) resource;
        final MRTableShard prodShard = prod.resolve(resourceName);
        if (!prodShard.isAvailable())
          throw new RuntimeException("Resource is not available at production: " + resourceName + " as " + prodShard.toString());
        prod.env().sample(prodShard, new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            builder.append(arg).append('\n');
          }
        });
        test.env().write(table, new CharSeqReader(builder));
      }
    }
    while (current.get(goal) == null || bannedResources.contains(goal)) {
      MRState next = current;
      for (int i = 0; i < jobs.size(); i++) {
        final MRJoba mrJoba = jobs.get(i);
        if (current.availableAll(mrJoba.produces()))
          continue;
        boolean consumesBanned = false;
        for (String resource : mrJoba.consumes()) {
          if (bannedResources.contains(resource))
            consumesBanned = true;
        }
        if (consumesBanned)
          continue;
        boolean producesBanned = false;
        for (String resource : mrJoba.produces()) {
          if (bannedResources.contains(resource))
            producesBanned = true;
        }

        if (producesBanned || !current.availableAll(mrJoba.produces())) {
          boolean needToRunProd = prod.checkAll(mrJoba.produces());

          if (!needToRunProd) { // performing a test run
            final Map<String, String> crcs = new HashMap<>();
            for (final String productName : mrJoba.produces()) {
              final Object product = test.resolve(productName);
              if (product instanceof MRTable) {
                final MRTable productTable = (MRTable) product;
                crcs.put(productTable.name(), productTable.crc(test.env()));
              }
            }
            if (!mrJoba.run(test))
              throw new RuntimeException("MR job failed in test environment: " + mrJoba.toString());
            // checking what have been produced
            for (final String productName : mrJoba.produces()) {
              if (!next.available(productName))
                throw new RuntimeException("MR job " + mrJoba.toString() + " failed to produce resource " + productName);
              bannedResources.remove(productName);
              final Object product = test.refresh(productName);
              if (product instanceof MRTable) {
                if (!((MRTable)product).crc(test.env()).equals(crcs.get(productName))) {
                  needToRunProd = true;
                }
              }
            }
            next = test.slice();
          }
          if (needToRunProd && !mrJoba.run(prod)) {
            throw new RuntimeException("MR job failed at production: " + mrJoba.toString());
          }
        }
      }
      if (current.equals(next)) {
        final StringBuilder message = new StringBuilder();
        message.append("MRProcess execution has stuck. ");

        final Set<Object> consumes = new HashSet<>();
        final Set<Object> produces = new HashSet<>();
        for (int i = 0; i < jobs.size(); i++) {
          final MRJoba mrJoba = jobs.get(i);
          for (int j = 0; j < mrJoba.consumes().length; j++) {
            consumes.add(test.resolve(mrJoba.consumes()[j]));
          }
          for (int j = 0; j < mrJoba.produces().length; j++) {
            produces.add(test.resolve(mrJoba.produces()[j]));
          }
        }
        consumes.removeAll(produces);
        if (!consumes.isEmpty()) {
          message.append("No way to get:");
          for (final Object consume : consumes) {
            message.append("\n\t").append(consume);
          }
        } else
          message.append("Cycle dependency!");

        throw new RuntimeException(message.toString());
      }
      current = next;
    }
    return current;
  }

  public void addJob(MRJoba job) {
    jobs.add(job);
  }

  @Override
  public <T> T result() {
    final MRState finalState = execute();
    return finalState.get(goal);
  }
}
