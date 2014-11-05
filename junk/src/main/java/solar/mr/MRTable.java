package solar.mr;


import com.spbsu.commons.func.Processor;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:36
 */
public interface MRTable {
  String name();

  boolean available(MREnv env);
  String crc(MREnv env);
  void read(MREnv env, final Processor<CharSequence> sapp);
  void sample(MREnv env, Processor<CharSequence> proc);
  void sort(MREnv env);
  void delete(MREnv env);

  abstract class Stub implements MRTable {
    @Override
    public boolean available(final MREnv env) {
      final MRTableShard[] shards = env.shards(this);
      for (MRTableShard shard : shards) {
        if (!shard.isAvailable())
          return false;
      }
      return true;
    }

    @Override
    public String crc(final MREnv env) {
      final StringBuilder crc = new StringBuilder();
      for (MRTableShard shard : env.shards(this)) {
        crc.append(shard.crc());
      }
      return crc.toString();
    }

    @Override
    public void read(final MREnv env, final Processor<CharSequence> proc) {
      for (MRTableShard shard : env.shards(this)) {
        env.read(shard, proc);
      }
    }

    @Override
    public void sample(final MREnv env, final Processor<CharSequence> proc) {
      for (MRTableShard shard : env.shards(this)) {
        env.sample(shard, proc);
      }
    }

    @Override
    public void sort(final MREnv env) {
      for (MRTableShard shard : env.shards(this)) {
        env.sort(shard);
      }
    }

    @Override
    public void delete(final MREnv env) {
      for (MRTableShard shard : env.shards(this)) {
        env.delete(shard);
      }
    }

  }
}
