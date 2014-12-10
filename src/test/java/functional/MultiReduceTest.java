package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.routines.MRRecord;

import java.util.*;

import static solar.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class MultiReduceTest extends BaseMRTest {

  private final Record[] RECORDS_1 = createRecordsWithKeys(9, 10, "key1", "key2", "key3");
  private final Record[] RECORDS_2 = createRecordsWithKeys(7, 20, "key3", "key4", "key5");

  private static final String IN_TABLE_NAME_1 = TABLE_NAME_PREFIX + "MultiReduceTest-in-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = TABLE_NAME_PREFIX + "MultiReduceTest-in-2-" + SALT;
  private static final String OUT_TABLE_NAME_1 = TABLE_NAME_PREFIX + "MultiReduceTest-out-1-" + SALT;
  private static final String OUT_TABLE_NAME_2 = TABLE_NAME_PREFIX + "MultiReduceTest-out-2-" + SALT;

  @Before
  public void createTables() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1
  })
  public static final class Reduce2in1 {

    public Reduce2in1(MRState state) {
    }

    @MRReduceMethod(input = SCHEMA + IN_TABLE_NAME_1, output = SCHEMA + OUT_TABLE_NAME_1)
    public void reduce1(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        output.add(record.key, record.sub, record.value);
      }
    }

    @MRReduceMethod(input = SCHEMA + IN_TABLE_NAME_2, output = SCHEMA + OUT_TABLE_NAME_1)
    public void reduce2(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        output.add(record.key, record.sub, record.value);
      }
    }

  }

  @Test
  public void reduceTwoInputsToOneOutput() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reduce2in1.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<Record> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS_1.length + RECORDS_2.length, records.size());
    Set<Record> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet.containsAll(records));
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1,
      SCHEMA + OUT_TABLE_NAME_2
  })
  public static final class Reduce2in2 {

    public Reduce2in2(MRState state) {
    }

    @MRReduceMethod(
        input = {
            SCHEMA + IN_TABLE_NAME_1,
            SCHEMA + IN_TABLE_NAME_2
        },
        output = {
            SCHEMA + OUT_TABLE_NAME_1,
            SCHEMA + OUT_TABLE_NAME_2
        })
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        if (record.sub.startsWith("subkey1")) {
          output.add(0, record.key, record.sub, record.value);
        } else if (record.sub.startsWith("subkey2")) {
          output.add(1, record.key, record.sub, record.value);
        } else {
          output.error("Unexpected key", "#", new MRRecord("#", record.key, record.sub, record.value));
        }
      }
    }

  }

  @Test
  public void reduceTwoInputsToTwoOutputs() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reduce2in2.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<Record> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<Record> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(RECORDS_1.length, records1.size());
    assertEquals(RECORDS_2.length, records2.size());
    Set<Record> recordsSet1 = new HashSet<>(Arrays.asList(RECORDS_1));
    assertTrue(recordsSet1.containsAll(records1));
    Set<Record> recordsSet2 = new HashSet<>(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet2.containsAll(records2));
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1,
      SCHEMA + OUT_TABLE_NAME_2
  })
  public static final class Reduce1in2 {

    public Reduce1in2(MRState state) {
    }

    @MRReduceMethod(
        input = {
            SCHEMA + IN_TABLE_NAME_1
        },
        output = {
            SCHEMA + OUT_TABLE_NAME_1,
            SCHEMA + OUT_TABLE_NAME_2
        })
    public void map(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        if (record.sub.equals("subkey10") || record.sub.equals("subkey11") || record.sub.equals("subkey12")) {
          output.add(0, record.key, record.sub, record.value);
        } else {
          output.add(1, record.key, record.sub, record.value);
        }
      }
    }

  }

  @Test
  public void reduceOneInputToTwoOutputs() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reduce1in2.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<Record> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<Record> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(9, records1.size());
    assertEquals(RECORDS_1.length - 9, records2.size());
    for(Record record: records1) {
      if (record.subkey.equals("subkey10") || record.subkey.equals("subkey11") || record.subkey.equals("subkey12")) {
        continue;
      }
      assertTrue(false);
    }
  }

  @After
  public void dropTables() {
    dropMRTable(env, IN_TABLE_NAME_1);
    dropMRTable(env, IN_TABLE_NAME_2);
    dropMRTable(env, OUT_TABLE_NAME_1);
    dropMRTable(env, OUT_TABLE_NAME_2);
  }

}
