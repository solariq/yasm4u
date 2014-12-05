package functional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;

import java.text.SimpleDateFormat;
import java.util.*;

import static functional.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class VarsTest extends BaseMRTest {

  private final MRTestUtils.Record[] RECORDS = createRecords(3);

  private static final String IN_TABLE_NAME_1 = TABLE_NAME_PREFIX + "VarsTest-1-1-" + SALT;
  private static final String OUT_TABLE_NAME_1 = TABLE_NAME_PREFIX + "VarsTest-1-2-" + SALT;

  private static final String IN_TABLE_NAME_2 = TABLE_NAME_PREFIX + "VarsTest-2-1-" + SALT + "/{var:date,date,yyyyMMdd}/{var:int}";
  private static final String OUT_TABLE_NAME_2 = TABLE_NAME_PREFIX + "VarsTest-2-2-" + SALT + "/{var:str}";

  private static final String IN_TABLE_NAME_3 = TABLE_NAME_PREFIX + "VarsTest-3-1-" + SALT;
  private static final String OUT_TABLE_NAME_3 = TABLE_NAME_PREFIX + "VarsTest-3-2-" + SALT + "/{var:array}";
  private static final String[] ARRAY_VALS = {"0", "1"};

  private static final String INT_VAR = "var:int";
  private static final String STRING_VAR = "var:str";
  private static final String DATE_VAR = "var:date";
  private static final String ARRAY_VAR = "var:array";

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME_1)
  public static final class Map1 {

    private final MRState state;

    public Map1(MRState state) {
      this.state = state;
    }

    @MRMapMethod(
        input = {
          SCHEMA + IN_TABLE_NAME_1,
          INT_VAR,
          STRING_VAR
        },
        output = {
            SCHEMA + OUT_TABLE_NAME_1
        }
    )
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      int i = state.get(INT_VAR);
      String s = state.get(STRING_VAR);
      output.add(key, sub, s + i);
    }

  }

  @Test
  public void varsPassingShouldWork() {
    final int INT_VAL = 123;
    final String STRING_VAL = "test";
    writeRecords(env, IN_TABLE_NAME_1, RECORDS);
    Properties vars = new Properties();
    vars.put(INT_VAR, INT_VAL);
    vars.put(STRING_VAR, STRING_VAL);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map1.class, env, vars);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRTestUtils.Record> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS.length, records.size());
    for(Record record: records) {
      assertEquals(STRING_VAL + INT_VAL, record.value);
    }
    dropMRTable(env, IN_TABLE_NAME_1);
    dropMRTable(env, OUT_TABLE_NAME_1);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME_2)
  public static final class Map2 {

    public Map2(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_2, output = SCHEMA + OUT_TABLE_NAME_2)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      output.add(key, sub, value);
    }

  }

  @Test
  public void varsInNamesShouldWork() {
    final int INT_VAL = 123;
    final String STRING_VAL = "test";
    final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    final Date DATE_VAL = new Date();
    final String REAL_IN_TABLE_NAME_2 = IN_TABLE_NAME_2.replace("{" + INT_VAR + "}", "" + INT_VAL).replace("{" + STRING_VAR + "}", STRING_VAL).replace("{" + DATE_VAR + ",date,yyyyMMdd}", format.format(DATE_VAL));
    final String REAL_OUT_TABLE_NAME_2 = OUT_TABLE_NAME_2.replace("{" + INT_VAR + "}", "" + INT_VAL).replace("{" + STRING_VAR + "}", STRING_VAL).replace("{" + DATE_VAR + ",date,yyyyMMdd}", format.format(DATE_VAL));
    writeRecords(env, REAL_IN_TABLE_NAME_2, RECORDS);
    Properties vars = new Properties();
    vars.put(INT_VAR, INT_VAL);
    vars.put(STRING_VAR, STRING_VAL);
    vars.put(DATE_VAR, DATE_VAL);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map2.class, env, vars);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRTestUtils.Record> records = readRecords(env, REAL_OUT_TABLE_NAME_2);
    assertEquals(RECORDS.length, records.size());
    Set<Record> recordsSet = new HashSet<>(Arrays.asList(RECORDS));
    assertTrue(recordsSet.containsAll(records));
    dropMRTable(env, REAL_IN_TABLE_NAME_2);
    dropMRTable(env, REAL_OUT_TABLE_NAME_2);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME_3)
  public static final class Map3 {

    public Map3(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_3, output = SCHEMA + OUT_TABLE_NAME_3)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      for(String i: ARRAY_VALS) {
        output.add(Integer.parseInt(i), key, sub, value);
      }
    }

  }

  @Test
  public void arraysInNamesShouldWork() {
    writeRecords(env, IN_TABLE_NAME_3, RECORDS);
    Properties vars = new Properties();
    vars.put(ARRAY_VAR, ARRAY_VALS);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map3.class, env, vars);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    for(String i: ARRAY_VALS) {
      final String table = OUT_TABLE_NAME_3.replace("{" + ARRAY_VAR + "}", i);
      List<MRTestUtils.Record> records = readRecords(env, table);
      assertEquals(RECORDS.length, records.size());
      Set<Record> recordsSet = new HashSet<>(Arrays.asList(RECORDS));
      assertTrue(recordsSet.containsAll(records));
      dropMRTable(env, table);
    }
    dropMRTable(env, IN_TABLE_NAME_3);
  }

}
