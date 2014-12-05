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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static functional.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class AsteriskTest extends BaseMRTest {

  private final MRTestUtils.Record[] RECORDS_1 = createRecords(10, 3);
  private final MRTestUtils.Record[] RECORDS_2 = createRecords(20, 3);
  private final MRTestUtils.Record[] RECORDS_3 = createRecords(30, 3);

  private static final String IN_TABLES = TABLE_NAME_PREFIX + "AsteriskTest/";
  private static final String IN_TABLE_NAME_1 = IN_TABLES + "AsteriskTest-1-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = IN_TABLES + "AsteriskTest-1-2-" + SALT;
  private static final String IN_TABLE_NAME_3 = IN_TABLES + "AsteriskTest-1-3-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "AsteriskTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
    writeRecords(env, IN_TABLE_NAME_3, RECORDS_3);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class Map {

    public Map(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLES + "*", output = SCHEMA + OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      output.add(key, sub, value);
    }

  }

  @Test
  public void asteriskShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRTestUtils.Record> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(RECORDS_1.length + RECORDS_2.length + RECORDS_3.length, records.size());
    Set<Record> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    recordsSet.addAll(Arrays.asList(RECORDS_3));
    assertTrue(recordsSet.containsAll(records));
  }

  @After
  public void dropTable() {
    dropMRTable(env, IN_TABLE_NAME_1);
    dropMRTable(env, IN_TABLE_NAME_2);
    dropMRTable(env, IN_TABLE_NAME_3);
    dropMRTable(env, OUT_TABLE_NAME);
  }

}
