package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRRoutine;
import solar.mr.MRTableShard;
import solar.mr.proc.State;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

import java.io.Reader;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * User: inikifor
 * Date: 30.11.14
 * Time: 16:27
 */
public final class ProfilerMREnv implements MREnv {
  public static final String PROFILER_SHARD_NAME = "temp:mr://profiler-shard";
  public static final String PROFILER_ENABLED_VAR = "var:profiling-enabled";
  private final MREnv wrapped;
  private final Map<Operation, Long> time = new EnumMap<>(Operation.class);
  private final Map<Operation, Integer> counter = new EnumMap<>(Operation.class);
  private final Map<String, Long> mapHostsTime = new HashMap<>();
  private final Map<String, Long> reduceHostsTime = new HashMap<>();
  private final Whiteboard wb;
  private int profilingOverhead;

  public ProfilerMREnv(MREnv wrapped, Whiteboard wb) {
    this.wb = wb;
    wb.set(PROFILER_ENABLED_VAR, true);
    this.wrapped = wrapped;
    for (Operation operation : Operation.values()) {
      time.put(operation, 0l);
      counter.put(operation, 0);
    }
  }

  public ProfilerMREnv(MREnv env) {
    this(env, new WhiteboardImpl(env, "profiling", System.getenv("USER")));
    wb.wipe();
  }

  public String tempPrefix() {
    return wrapped.tempPrefix();
  }

  @Override
  public boolean execute(Class<? extends MRRoutine> exec, State state, MRTableShard[] in, MRTableShard[] out, MRErrorsHandler errorsHandler) {
    long start = System.currentTimeMillis();
    final Operation execOperation;
    if (MRMap.class.isAssignableFrom(exec)) {
      execOperation = Operation.MAP;
    } else if (MRReduce.class.isAssignableFrom(exec)) {
      execOperation = Operation.REDUCE;
    } else {
      execOperation = Operation.UNKNOWN_ROUTINE;
    }

    final MRTableShard profilerShard = wb.get(PROFILER_SHARD_NAME);
    assert profilerShard != null;
    final MRTableShard[] alteredOut = new MRTableShard[out.length + 1];
    System.arraycopy(out, 0, alteredOut, 0, out.length);
    alteredOut[out.length] = profilerShard;

    boolean result = wrapped.execute(exec, state, in, alteredOut, errorsHandler);
    final Map<String, Long> stat = new HashMap<>();
    wrapped.read(profilerShard, new MRRoutine(new String[]{profilerShard.path()}, null, state) {
      @Override
      public void invoke(final MRRecord record) {
        stat.put(record.key, Long.parseLong(record.value.toString()));
      }
    });
    incrementTime(execOperation, System.currentTimeMillis() - start);
    if (execOperation != Operation.UNKNOWN_ROUTINE)
      mergeMaps(execOperation == Operation.MAP? mapHostsTime: reduceHostsTime, stat);
    return result;
  }

  @Override
  public MRTableShard resolve(String path)
  {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.resolve(path);
    }
    finally {
      incrementTime(Operation.RESOLVE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard[] resolveAll(String... paths) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.resolveAll(paths);
    }
    finally {
      incrementTime(Operation.RESOLVE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public int read(MRTableShard shard, Processor<CharSequence> seq) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.read(shard, seq);
    }
    finally {
      incrementTime(Operation.READ, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard write(MRTableShard shard, Reader content) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.write(shard, content);
    }
    finally {
      incrementTime(Operation.WRITE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard append(MRTableShard shard, Reader content) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.append(shard, content);
    }
    finally {
      incrementTime(Operation.APPEND, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void sample(MRTableShard shard, Processor<CharSequence> seq) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.sample(shard, seq);
    }
    finally {
      incrementTime(Operation.SAMPLE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard[] list(String prefix) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.list(prefix);
    }
    finally {
      incrementTime(Operation.LIST, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard copy(MRTableShard[] from, MRTableShard to, boolean append) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.copy(from, to, append);
    }
    finally {
      incrementTime(Operation.COPY, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard delete(MRTableShard shard) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.delete(shard);
    }
    finally {
      incrementTime(Operation.DELETE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public MRTableShard sort(MRTableShard shard) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.delete(shard);
    }
    finally {
      incrementTime(Operation.SORT, System.currentTimeMillis() - start);
    }
  }

  @Override
  public String name() {
    return wrapped.name();
  }

  @Override
  public void addListener(Action<? super ShardAlter> lst) {
    wrapped.addListener(lst);
  }

  @SuppressWarnings("UnusedDeclaration")
  public void reset() {
    time.clear();
    for (Operation operation : Operation.values()) {
      time.put(operation, 0l);
      counter.put(operation, 0);
    }
    mapHostsTime.clear();
    reduceHostsTime.clear();
    profilingOverhead = 0;
  }

  public void printStatistics() {
    int total = 0;
    for(Operation op: Operation.values()) {
      if (!op.isLibraryCall()) {
        long value = time.get(op) / 1000;
        System.out.printf("%s:\t%d sec per %d calls%n", op.toString(), value, counter.get(op));
        total += value;
      }
    }
    System.out.println("Total:\t" + total + " sec");
    System.out.println("\nMap per host:");
    printHostsStatistics(mapHostsTime);
    System.out.println("\nReduce per host:");
    printHostsStatistics(reduceHostsTime);
    System.out.println();
    total = 0;
    for(Operation op: Operation.values()) {
      if (op.isLibraryCall()) {
        long value = time.get(op) / 1000;
        System.out.printf("%s:\t%d sec per %d calls%n", op.toString(), value, counter.get(op));
        total += value;
      }
    }
    System.out.println("Total:\t" + total + " sec");
    System.out.println();
    System.out.printf("Profiling overhead: %d sec%n", profilingOverhead / 1000);
  }

  private void printHostsStatistics(Map<String, Long> hostsTime) {
    for (final Map.Entry<String, Long> hostTime : hostsTime.entrySet()) {
      System.out.printf("\t%s: %d sec%n", hostTime.getKey(), hostTime.getValue() / 1000);
    }
  }

  private void mergeMaps(Map<String, Long> hostsTime, Map<String, Long> hosts) {
    for (final Map.Entry<String, Long> hostTime : hosts.entrySet()) {
      if (!hostsTime.containsKey(hostTime.getKey())) {
        hostsTime.put(hostTime.getKey(), hostTime.getValue());
      } else {
        hostsTime.put(hostTime.getKey(), hostsTime.get(hostTime.getKey()) + hostTime.getValue());
      }
    }
  }

  private void incrementTime(Operation op, long delta) {
    time.put(op, time.get(op) + delta);
    counter.put(op, counter.get(op) + 1);
  }


  public enum Operation {
    MAP(false),
    REDUCE(false),
    UNKNOWN_ROUTINE(false),
    SORT(false),
    READ(true),
    WRITE(true),
    COPY(true),
    DELETE(true),
    LIST(true),
    APPEND(true),
    SAMPLE(true),
    RESOLVE(true);

    private final boolean libraryCall;

    Operation(boolean libraryCall) {
      this.libraryCall = libraryCall;
    }

    public boolean isLibraryCall() {
      return libraryCall;
    }
  }

}
