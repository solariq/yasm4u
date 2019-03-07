package com.expleague.yasm4u.domains.mr.env;

import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.io.Reader;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * User: inikifor
 * Date: 30.11.14
 * Time: 16:27
 */
public final class ProfilerMREnv extends MREnvBase {
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
      time.put(operation, 0L);
      counter.put(operation, 0);
    }
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, MRErrorsHandler errorsHandler) {
    long start = System.currentTimeMillis();
    final MRPath profilerShard = wb.get(PROFILER_SHARD_NAME);
    assert profilerShard != null;
    builder.addOutput(profilerShard);
    boolean result = wrapped.execute(builder, errorsHandler);
    final Map<String, Long> stat = new HashMap<>();
    wrapped.read(profilerShard, new MROperation(new MRPath[]{profilerShard}, null, null) {
      @Override
      public void accept(final MRRecord record) {
        stat.put(record.key, Long.parseLong(record.value.toString()));
      }
    });
    final Operation routineType = convertToOperation(builder.getRoutineType());
    incrementTime(routineType, System.currentTimeMillis() - start);
    mergeMaps(routineType == Operation.MAP ? mapHostsTime : reduceHostsTime, stat);
    return result;
  }

  private Operation convertToOperation(MRRoutineBuilder.RoutineType type) {
    switch (type) {
      case MAP:
        return Operation.MAP;
      case REDUCE:
        return Operation.REDUCE;
    }
    throw new IllegalStateException("Should never happen");
  }

  @Override
  public MRTableState resolve(MRPath path)
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
  public MRTableState[] resolveAll(MRPath... paths) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.resolveAll(paths);
    }
    finally {
      incrementTime(Operation.RESOLVE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public int read(MRPath shard, Consumer<MRRecord> seq) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.read(shard, seq);
    }
    finally {
      incrementTime(Operation.READ, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void write(MRPath shard, Reader content) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.write(shard, content);
    }
    finally {
      incrementTime(Operation.WRITE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void append(MRPath shard, Reader content) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.append(shard, content);
    }
    finally {
      incrementTime(Operation.APPEND, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void sample(MRPath shard, Consumer<MRRecord> seq) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.sample(shard, seq);
    }
    finally {
      incrementTime(Operation.SAMPLE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void update(MRPath prefix) {
    if (prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be table");
    list(prefix.parent());
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    final long start = System.currentTimeMillis();
    try {
      return wrapped.list(prefix);
    }
    finally {
      incrementTime(Operation.LIST, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.copy(from, to, append);
    }
    finally {
      incrementTime(Operation.COPY, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void delete(MRPath shard) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.delete(shard);
    }
    finally {
      incrementTime(Operation.DELETE, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void sort(MRPath shard) {
    final long start = System.currentTimeMillis();
    try {
      wrapped.delete(shard);
    }
    finally {
      incrementTime(Operation.SORT, System.currentTimeMillis() - start);
    }
  }

  @Override
  public long key(MRPath shard, String key, Consumer<MRRecord> seq) {
    return 0;
  }

  @Override
  public String name() {
    return wrapped.name();
  }

  @SuppressWarnings("UnusedDeclaration")
  public void reset() {
    time.clear();
    for (Operation operation : Operation.values()) {
      time.put(operation, 0L);
      counter.put(operation, 0);
    }
    mapHostsTime.clear();
    reduceHostsTime.clear();
    profilingOverhead = 0;
  }

  @SuppressWarnings("UnusedDeclaration")
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
