package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import solar.mr.*;
import solar.mr.proc.MRState;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRReduce;

import java.io.Reader;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by inikifor on 30.11.14.
 */
public final class ProfilerMREnv implements MREnv {

  private static final String PROFILE_DATA_TABLE = "temp/profiler-" + Integer.toHexString(new FastRandom().nextInt());

  private final ProfilableMREnv wrapped;
  private final Map<Operation, Integer> time = new EnumMap<>(Operation.class);
  private final Map<String, Integer> mapHostsTime = new HashMap<>();
  private final Map<String, Integer> reduceHostsTime = new HashMap<>();
  private int profilingOverhead;

  public ProfilerMREnv(ProfilableMREnv wrapped) {
    this.wrapped = wrapped;
    for (Operation operation : Operation.values()) {
      time.put(operation, 0);
    }
  }

  @Override
  public boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTableShard[] in, MRTableShard[] out, MRErrorsHandler errorsHandler) {
    long start = System.currentTimeMillis();
    final Map<String, Integer> hosts = new HashMap<>();
    final int[] times = new int[2];
    boolean result = wrapped.execute(exec, state, in, out, errorsHandler, new ProfilableMREnv.Profiler() {

      @Override
      public boolean isEnabled() {
        return true;
      }

      @Override
      public String getTableName() {
        return PROFILE_DATA_TABLE;
      }

      @Override
      public void addExecutionStatistics(Map<String, Integer> timePerHosts) {
        hosts.putAll(timePerHosts);
      }

      @Override
      public int profilableRead(MRTableShard shard, Processor<CharSequence> seq) {
        long start = System.currentTimeMillis();
        int result = ProfilerMREnv.this.read(shard, seq);
        times[0] += (int) (System.currentTimeMillis() - start);
        return result;
      }

      @Override
      public void profilableDelete(MRTableShard shard) {
        long start = System.currentTimeMillis();
        ProfilerMREnv.this.delete(shard);
        times[1] += (int) (System.currentTimeMillis() - start);
      }

      @Override
      public MRTableShard[] profilableList(String prefix) {
        return ProfilerMREnv.this.list(prefix);
      }
    });
    int t = (int) (System.currentTimeMillis() - start) - times[0] - times[1];
    if (MRMap.class.isAssignableFrom(exec)) {
      incrementTime(Operation.MAP, t);
      mergeMaps(mapHostsTime, hosts);
    } else if (MRReduce.class.isAssignableFrom(exec)) {
      incrementTime(Operation.REDUCE, t);
      mergeMaps(reduceHostsTime, hosts);
    } else {
      throw new RuntimeException("Unexpected routine");
    }
    return result;
  }

  private void mergeMaps(Map<String, Integer> hostsTime, Map<String, Integer> hosts) {
    for (Map.Entry<String, Integer> hostTime : hosts.entrySet()) {
      if (!hostsTime.containsKey(hostTime.getKey())) {
        hostsTime.put(hostTime.getKey(), hostTime.getValue());
      } else {
        hostsTime.put(hostTime.getKey(), hostsTime.get(hostTime.getKey()) + hostTime.getValue());
      }
    }
  }

  @Override
  public MRTableShard resolve(String path) {
    return wrapped.resolve(path);
  }

  @Override
  public MRTableShard[] resolveAll(String[] strings) {
    return wrapped.resolveAll(strings);
  }

  private void incrementTime(Operation op, int delta) {
    time.put(op, time.get(op) + delta);
  }

  @Override
  public int read(MRTableShard shard, Processor<CharSequence> seq) {
    long start = System.currentTimeMillis();
    int result = wrapped.read(shard, seq);
    if (shard.path().contains(PROFILE_DATA_TABLE)) {
      profilingOverhead += (int) (System.currentTimeMillis() - start);
    } else {
      incrementTime(Operation.READ, (int) (System.currentTimeMillis() - start));
    }
    return result;
  }

  @Override
  public void write(MRTableShard shard, Reader content) {
    long start = System.currentTimeMillis();
    wrapped.write(shard, content);
    incrementTime(Operation.WRITE, (int) (System.currentTimeMillis() - start));
  }

  @Override
  public void append(MRTableShard shard, Reader content) {
    long start = System.currentTimeMillis();
    wrapped.append(shard, content);
    incrementTime(Operation.APPEND, (int) (System.currentTimeMillis() - start));
  }

  @Override
  public void sample(MRTableShard shard, Processor<CharSequence> seq) {
    long start = System.currentTimeMillis();
    wrapped.sample(shard, seq);
    incrementTime(Operation.SAMPLE, (int) (System.currentTimeMillis() - start));
  }

  @Override
  public MRTableShard[] list(String prefix) {
    long start = System.currentTimeMillis();
    MRTableShard[] result = wrapped.list(prefix);
    incrementTime(Operation.LIST, (int) (System.currentTimeMillis() - start));
    return result;
  }

  @Override
  public void copy(MRTableShard[] from, MRTableShard to, boolean append) {
    long start = System.currentTimeMillis();
    wrapped.copy(from, to, append);
    incrementTime(Operation.COPY, (int) (System.currentTimeMillis() - start));
  }

  @Override
  public void delete(MRTableShard shard) {
    long start = System.currentTimeMillis();
    wrapped.delete(shard);
    if (shard.path().contains(PROFILE_DATA_TABLE)) {
      profilingOverhead += (int) (System.currentTimeMillis() - start);
    } else {
      incrementTime(Operation.DELETE, (int) (System.currentTimeMillis() - start));
    }
  }

  @Override
  public MRTableShard sort(MRTableShard shard) {
    long start = System.currentTimeMillis();
    MRTableShard result = wrapped.sort(shard);
    incrementTime(Operation.SORT, (int) (System.currentTimeMillis() - start));
    return result;
  }

  @Override
  public String name() {
    return wrapped.name();
  }

  @Override
  public void addListener(Action<? super ShardAlter> lst) {
    wrapped.addListener(lst);
  }

  public Map<Operation, Integer> getExecutionTime() {
    return Collections.unmodifiableMap(time);
  }

  public int getProfilingOverhead() {
    return profilingOverhead;
  }

  public void reset() {
    time.clear();
    for (Operation operation : Operation.values()) {
      time.put(operation, 0);
    }
    mapHostsTime.clear();
    reduceHostsTime.clear();
    profilingOverhead = 0;
  }

  public void printStatistics() {
    int total = 0;
    for(Operation op: Operation.values()) {
      if (!op.isLibraryCall()) {
        int value = time.get(op) / 1000;
        System.out.printf("%s:\t%d sec%n", op.toString(), value);
        total += value;
      }
    }
    System.out.println("Total: " + total + " sec");
    System.out.println("\nMap per host:");
    printHostsStatistics(mapHostsTime);
    System.out.println("\nReduce per host:");
    printHostsStatistics(reduceHostsTime);
    System.out.println();
    total = 0;
    for(Operation op: Operation.values()) {
      if (op.isLibraryCall()) {
        int value = time.get(op) / 1000;
        System.out.printf("%s:\t%d sec%n", op.toString(), value);
        total += value;
      }
    }
    System.out.println("Total: " + total + " sec");
    System.out.println();
    System.out.printf("Profiling overhead: %d sec%n", profilingOverhead / 1000);
  }

  private void printHostsStatistics(Map<String, Integer> hostsTime) {
    for (Map.Entry<String, Integer> hostTime : hostsTime.entrySet()) {
      System.out.printf("\t%s: %d sec%n", hostTime.getKey(), hostTime.getValue() / 1000);
    }
  }

  public enum Operation {
    MAP(false),
    REDUCE(false),
    SORT(false),
    READ(true),
    WRITE(true),
    COPY(true),
    DELETE(true),
    LIST(true),
    APPEND(true),
    SAMPLE(true);

    private final boolean libraryCall;

    Operation(boolean libraryCall) {
      this.libraryCall = libraryCall;
    }

    public boolean isLibraryCall() {
      return libraryCall;
    }
  }

}
