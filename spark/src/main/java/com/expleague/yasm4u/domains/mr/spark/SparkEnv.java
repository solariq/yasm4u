package com.expleague.yasm4u.domains.mr.spark;

import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.env.MRRunner;
import com.expleague.yasm4u.domains.mr.env.ProcessRunner;
import com.expleague.yasm4u.domains.mr.env.RemoteMREnv;
import com.expleague.yasm4u.domains.mr.env.SSHProcessRunner;
import com.expleague.yasm4u.domains.mr.ops.MRMap;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import com.expleague.yasm4u.domains.wb.impl.StateImpl;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

public class SparkEnv extends RemoteMREnv {

  public SparkEnv(ProcessRunner runner, String user, String master) {
    super(runner, user, master, System.err::println, System.out::println, SparkRunner.class);
  }

  @Override
  public boolean execute(MRRoutineBuilder exec, MRErrorsHandler errorsHandler, File jar) {
    List<String> options = Arrays.asList(
        "--class", SparkRunner.class.getName(),
        "--deploy-mode", "client",
        "--master", "yarn",
        "--executor-memory", "3G",
        "--driver-memory", "2G",
        jar.getAbsolutePath());
    //noinspection ArraysAsListWithZeroOrOneArgument
    executeCommand(options, new HashSet<>(Arrays.asList(jar.getAbsolutePath())), System.out::println, System.err::println, null);
    return true;
  }

  @Override
  protected MRPath findByLocalPath(String table, boolean sorted) {
    return null;
  }

  @Override
  protected String localPath(MRPath shard) {
    return null;
  }

  @Override
  protected boolean isFat(MRPath path) {
    return false;
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    return new MRPath[0];
  }

  @Override
  public void update(MRPath prefix) {

  }

  @Override
  public int read(MRPath shard, Consumer<MRRecord> seq) {
    return 0;
  }

  @Override
  public void sample(MRPath shard, Consumer<MRRecord> seq) {

  }

  @Override
  public void write(MRPath shard, Reader content) {

  }

  @Override
  public void append(MRPath shard, Reader content) {

  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {

  }

  @Override
  public void delete(MRPath shard) {

  }

  @Override
  public void sort(MRPath shard) {

  }

  @Override
  public long key(MRPath shard, String key, Consumer<MRRecord> seq) {
    return 0;
  }

  @Override
  public String name() {
    return null;
  }

  public static void main(String[] args) {
    final S3Path input = new S3Path(MRPath.Mount.ROOT, "joom.emr.fs", "/home/solar/day_cohort_7first_days");
    final S3Path output = new S3Path(MRPath.Mount.ROOT, "joom.emr.fs", "/home/solar/day_cohort_7first_days_out");
    final SSHProcessRunner runner = new SSHProcessRunner("hadoop@10.0.115.86", "/usr/bin/spark-submit");
    runner.setPrivateKey("/Users/solar/.ssh/id_rsa.hadoop");
    final SparkEnv env = new SparkEnv(runner, "", "");
    final MRRoutineBuilder builder = new MRRoutineBuilder() {
      @Override
      public RoutineType getRoutineType() {
        return RoutineType.MAP;
      }

      @Override
      public MROperation build(MROutput output) {
        return new MyMRMap(input, output);
      }
    };
    builder.addInput(input);
    builder.addOutput(output);

    env.execute(builder, new DefaultMRErrorsHandler());
  }

  private static class MyMRMap extends MRMap {
    public MyMRMap(S3Path input, MROutput output) {
      super(new MRPath[]{input}, output, new StateImpl());
    }

    @Override
    public void map(MRPath table, String sub, CharSequence value, String key) {
      output.add("hello", "world", "!");
    }
  }
}
