package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.AnnotatedMRProcess;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.routines.MRRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static solar.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 03.12.14.
 */
@RunWith(Parameterized.class)
public final class MultiMapTest extends BaseMRTest {

  private final MRRecord[] RECORDS_1 = createRecords(10, 9);
  private final MRRecord[] RECORDS_2 = createRecords(20, 7);

  private static final String IN_TABLE_NAME_1 = TABLE_NAME_PREFIX + "MultiMapTest-in-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = TABLE_NAME_PREFIX + "MultiMapTest-in-2-" + SALT;
  private static final String OUT_TABLE_NAME_1 = TABLE_NAME_PREFIX + "MultiMapTest-out-1-" + SALT;
  private static final String OUT_TABLE_NAME_2 = TABLE_NAME_PREFIX + "MultiMapTest-out-2-" + SALT;


  @Before
  public void createTables() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1
  })
  public static final class Map2in1 {

    public Map2in1(State state) {
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
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS_1.length + RECORDS_2.length, records.size());
    Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet.containsAll(records));
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1,
      SCHEMA + OUT_TABLE_NAME_2
  })
  public static final class Map2in2 {

    public Map2in2(State state) {
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
    public void map(final MRPath table, final String key, final String sub, final CharSequence value, MROutput output) {
      if (key.startsWith("key1")) {
        output.add(0, key, sub, value);
      } else if (key.startsWith("key2")) {
        output.add(1, key, sub, value);
      } else {
        output.error("Unexpected key", "#", new MRRecord(table, key, sub, value));
      }
    }

  }

  @Test
  public void mapTwoInputsToTwoOutputs() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map2in2.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRRecord> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<MRRecord> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(RECORDS_1.length, records1.size());
    assertEquals(RECORDS_2.length, records2.size());
    Set<MRRecord> recordsSet1 = new HashSet<>(Arrays.asList(RECORDS_1));
    assertTrue(recordsSet1.containsAll(records1));
    Set<MRRecord> recordsSet2 = new HashSet<>(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet2.containsAll(records2));
  }

  @MRProcessClass(goal = {
      SCHEMA + OUT_TABLE_NAME_1,
      SCHEMA + OUT_TABLE_NAME_2
  })
  public static final class Map1in2 {

    public Map1in2(State state) {
    }

    @MRMapMethod(
        input = {
            SCHEMA + IN_TABLE_NAME_1
        },
        output = {
            SCHEMA + OUT_TABLE_NAME_1,
            SCHEMA + OUT_TABLE_NAME_2
        })
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (key.equals("key10") || key.equals("key11") || key.equals("key12")) {
        output.add(0, key, sub, value);
      } else {
        output.add(1, key, sub, value);
      }
    }

  }

  @Test
  public void mapOneInputToTwoOutputs() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map1in2.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRRecord> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<MRRecord> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(3, records1.size());
    assertEquals(RECORDS_1.length - 3, records2.size());
    for(MRRecord record: records1) {
      if (record.key.equals("key10") || record.key.equals("key11") || record.key.equals("key12")) {
        continue;
      }
      assertTrue(false);
    }
  }

  @After
  public void dropTables() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_1));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_2));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME_1));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME_2));
  }
}
