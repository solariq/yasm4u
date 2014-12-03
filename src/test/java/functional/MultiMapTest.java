package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.routines.MRRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static functional.MRTestUtils.*;
import static functional.MRTestUtils.dropMRTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 03.12.14.
 */
@RunWith(Parameterized.class)
public final class MultiMapTest extends BaseMRTest {

  private final MRTestUtils.Record[] RECORDS_1 = createRecords(10, 9);
  private final MRTestUtils.Record[] RECORDS_2 = createRecords(20, 7);

  private static final String IN_TABLE_NAME_1 = TABLE_NAME_PREFIX + "SimpleMapTest-in-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = TABLE_NAME_PREFIX + "SimpleMapTest-in-2-" + SALT;
  private static final String OUT_TABLE_NAME_1 = TABLE_NAME_PREFIX + "SimpleMapTest-out-1-" + SALT;
  private static final String OUT_TABLE_NAME_2 = TABLE_NAME_PREFIX + "SimpleMapTest-out-2-" + SALT;


  @Before
  public void createTables() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1
  })
  public static final class Map2in1 {

    public Map2in1(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_1, output = SCHEMA + OUT_TABLE_NAME_1)
    public void map1(final String key, final String sub, final CharSequence value, MROutput output) {
      output.add(key, sub, value);
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_2, output = SCHEMA + OUT_TABLE_NAME_1)
    public void map2(final String key, final String sub, final CharSequence value, MROutput output) {
      output.add(key, sub, value);
    }

  }

  @Test
  public void mapTwoInputsToOneOutput() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map2in1.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRTestUtils.Record> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS_1.length + RECORDS_2.length, records.size());
    Set<MRTestUtils.Record> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet.containsAll(records));
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1,
      SCHEMA + OUT_TABLE_NAME_2
  })
  public static final class Map2in2 {

    public Map2in2(MRState state) {
    }

    @MRMapMethod(
        input = {
            SCHEMA + IN_TABLE_NAME_1,
            SCHEMA + IN_TABLE_NAME_2
        },
        output = {
            SCHEMA + OUT_TABLE_NAME_1,
            SCHEMA + OUT_TABLE_NAME_2
        })
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (key.startsWith("key1")) {
        output.add(0, key, sub, value);
      } else if (key.startsWith("key2")) {
        output.add(1, key, sub, value);
      } else {
        output.error("Unexpected key", "#", new MRRecord("#", key, sub, value));
      }
    }

  }

  @Test
  public void mapTwoInputsToTwoOutputs() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map2in2.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRTestUtils.Record> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<MRTestUtils.Record> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(RECORDS_1.length, records1.size());
    assertEquals(RECORDS_2.length, records2.size());
    Set<MRTestUtils.Record> recordsSet1 = new HashSet<>(Arrays.asList(RECORDS_1));
    assertTrue(recordsSet1.containsAll(records1));
    Set<MRTestUtils.Record> recordsSet2 = new HashSet<>(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet2.containsAll(records2));
  }

  @After
  public void dropTables() {
    dropMRTable(env, IN_TABLE_NAME_1);
    dropMRTable(env, IN_TABLE_NAME_2);
    dropMRTable(env, OUT_TABLE_NAME_1);
    dropMRTable(env, OUT_TABLE_NAME_2);
  }
}