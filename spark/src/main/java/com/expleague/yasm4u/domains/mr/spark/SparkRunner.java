package com.expleague.yasm4u.domains.mr.spark;

import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.env.MROutputBase;
import com.expleague.yasm4u.domains.mr.env.MRRunner;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public final class SparkRunner extends MRRunner {
  @Override
  public void run() {
    final SparkSession spark = SparkSession
        .builder()
        .appName("YASM4U Runner: " + routineBuilder.toString())
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.hadoop.validateOutputSpecs", "false")
        .getOrCreate();

    final RecordsAccumulator accumulator = new RecordsAccumulator(routineBuilder.input(), routineBuilder.output());
    final MROperation instance = routineBuilder.build(accumulator);
    final CharSequence[] split = new CharSequence[3];
    Stream.of(routineBuilder.input()).forEach(input -> {
      final JavaRDD<String> rdd = spark.read().textFile(input.toURI().toString()).javaRDD();
      JavaRDD<Tuple2<String, CharSequence>> table2records = rdd.flatMap(line -> {
        final int parts = CharSeqTools.trySplit(line, '\t', split);
        if (parts == 3) {
          try {
            instance.accept(new MRRecord(input, split[0].toString(), split[1].toString(), split[3]));
          }
          catch (Exception e) {
            accumulator.error(e, instance.currentRecord());
          }
        }
        else
          accumulator.error("Illegal record", "Contains less then 3 fields!", new MRRecord(input, split[0].toString(), "", line));

        return accumulator.clean();
      });
      final MRPath[] output = routineBuilder.output();
      rdd.persist(StorageLevel.MEMORY_ONLY_2());
      Stream.of(output).parallel().forEach(anOutput -> {
        String currentOutput = anOutput.toString();
        table2records.filter(t -> t._1().equals(currentOutput)).map(Tuple2::_2).saveAsTextFile(currentOutput);
      });
    });
    spark.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 1 && "--dump".equals(args[0])) {
      buildJar(args[1], cls ->
          !cls.startsWith("org.apache.log4j") && !cls.startsWith("javassist") && !cls.startsWith("org.apache.spark") && !cls.startsWith("org.spark_project") && !cls.startsWith("scala")
      );
    }
    else new SparkRunner().run();
  }

  private static class RecordsAccumulator extends MROutputBase implements Serializable {
    private final List<Tuple2<String, CharSequence>> records = new ArrayList<>();
    private String[] outputNames;

    public RecordsAccumulator() {}

    public RecordsAccumulator(MRPath[] input, MRPath[] output) {
      super(input);
      outputNames = Stream.of(output).map(MRPath::toString).toArray(String[]::new);
    }

    @Override
    protected void push(int tableNo, CharSequence record) {
      records.add(Tuple2.apply(outputNames[tableNo], record));
    }

    public Iterator<Tuple2<String, CharSequence>> clean() {
      ArrayList<Tuple2<String, CharSequence>> copy = new ArrayList<>(records);
      records.clear();
      return copy.iterator();
    }
  }
}