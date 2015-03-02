package yamr;

import com.spbsu.commons.func.Processor;
import solar.mr.MRRoutine;
import solar.mr.env.LocalMREnv;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

/**
 * User: solar
 * Date: 02.03.15
 * Time: 10:23
 */
public class FakeMREnv extends LocalMREnv {
  public static final MRPath oneRecordShard = MRPath.create("/tmp/one-record");
  public static final MRPath oneMillionRecordShard = MRPath.create("/tmp/one-million-record");

  public FakeMREnv() {
    super(createTemp().home().getAbsolutePath());
  }

  @Override
  protected int read(MRPath shard, MRRoutine routine) {
    if (oneMillionRecordShard.equals(shard)) {
      for (int i = 0; i < 1_000_000; i++) {
        routine.invoke(i +"\t#\tvalue");
      }
      return 1_000_000;
    }
    if (oneRecordShard.equals(shard)) {
      routine.invoke(new MRRecord(oneRecordShard, "0", "#", "value").toCharSequence());
      return 1;
    }

    return super.read(shard, routine);
  }
}
