package solar.mr.proc.impl;

import java.util.*;


import solar.mr.MREnv;
import solar.mr.MRTable;
import solar.mr.env.LocalMREnv;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRProcess;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;

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
    final MRWhiteboard test = new MRWhiteboardImpl(cache, name() + "/" + prod.env().toString(), System.getenv("USER"));
    // at start banning all producible resources
    final Set<String> bannedResources = new HashSet<>();
    for (int j = 0; j < jobs.size(); j++) {
      bannedResources.addAll(Arrays.asList(jobs.get(j).produces()));
    }
    test.clear();
    MRState current = test.slice();
    while (current.get(goal) == null) {
      MRState next = current;
      for (int i = 0; i < jobs.size(); i++) {
        final MRJoba mrJoba = jobs.get(i);
        if (current.availableAll(mrJoba.consumes()))
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
                crcs.put(productTable.name(), productTable.crc(cache));
              }
            }
            if (!mrJoba.run(test))
              throw new RuntimeException("MR job failed in test environment: " + mrJoba.toString());
            next = test.slice();
            // checking what have been produced
            for (final String productName : mrJoba.produces()) {
              if (!next.available(productName))
                throw new RuntimeException("MR job " + mrJoba.toString() + " failed to produce resource " + productName);
              bannedResources.remove(productName);
              final Object product = test.resolve(productName);
              if (product instanceof MRTable) {
                if (!((MRTable) product).crc(cache).equals(crcs.get(productName))) {
                  needToRunProd = true;
                }
              }
            }
          }
          if (needToRunProd && !mrJoba.run(prod))
            throw new RuntimeException("MR job failed at production: " + mrJoba.toString());
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
