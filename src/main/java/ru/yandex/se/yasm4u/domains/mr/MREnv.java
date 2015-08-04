package ru.yandex.se.yasm4u.domains.mr;

import java.io.Reader;


import com.spbsu.commons.func.Processor;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;


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
  void get(MRPath prefix);

  int read(MRPath shard, Processor<MRRecord> seq);
  void sample(MRPath shard, Processor<MRRecord> seq);
  void write(MRPath shard, Reader content);
  void append(MRPath shard, Reader content);
  void copy(MRPath[] from, MRPath to, boolean append);
  void delete(MRPath shard);
  void sort(MRPath shard);
  long key(MRPath shard, String key, Processor<MRRecord> seq);

  String name();
}
