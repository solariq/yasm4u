package com.expleague.yasm4u.domains.mr;

import java.io.Reader;
import java.util.function.Consumer;


import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;


/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv extends Domain {
  boolean execute(MRRoutineBuilder exec, final MRErrorsHandler errorsHandler);

  MRTableState resolve(MRPath path);
  MRTableState[] resolveAll(MRPath... strings);

  MRPath[] list(MRPath prefix);
  void update(MRPath prefix);

  int read(MRPath shard, Consumer<MRRecord> seq);
  void sample(MRPath shard, Consumer<MRRecord> seq);
  void write(MRPath shard, Reader content);
  void append(MRPath shard, Reader content);
  void copy(MRPath[] from, MRPath to, boolean append);
  void delete(MRPath shard);
  void sort(MRPath shard);
  long key(MRPath shard, String key, Consumer<MRRecord> seq);

  String name();
}
