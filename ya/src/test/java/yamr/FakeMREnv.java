package yamr;

import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.mr.env.LocalMREnv;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

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
  protected int read(MRPath shard, MROperation routine) {
    if (oneMillionRecordShard.equals(shard)) {
      for (int i = 0; i < 1_000_000; i++) {
        routine.recordParser().accept(i +"\t#\tvalue");
      }
      return 1_000_000;
    }
    if (oneRecordShard.equals(shard)) {
      routine.recordParser().accept(new MRRecord(oneRecordShard, "0", "#", "value").toCharSequence());
      return 1;
    }

    return super.read(shard, routine);
  }
}
