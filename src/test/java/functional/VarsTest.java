package functional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.text.SimpleDateFormat;
import java.util.*;

import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class VarsTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(3);

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

  @MRProcessClass(goal = OUT_TABLE_NAME_1)
  public static final class Map1 {

    private final State state;

    public Map1(State state) {
      this.state = state;
    }

    @MRMapMethod(
        input = {
          IN_TABLE_NAME_1,
          INT_VAR,
          STRING_VAR
        },
        output = {
            OUT_TABLE_NAME_1
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
    Whiteboard vars = new WhiteboardImpl(env, Map1.class.getName());
    vars.set(INT_VAR, INT_VAL);
    vars.set(STRING_VAR, STRING_VAL);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map1.class, vars, env);
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS.length, records.size());
    for(MRRecord record: records) {
      assertEquals(STRING_VAL + INT_VAL, record.value.toString());
    }
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_1));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME_1));
  }

  @MRProcessClass(goal = OUT_TABLE_NAME_2)
  public static final class Map2 {

    public Map2(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME_2, output = OUT_TABLE_NAME_2)
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
    Whiteboard vars = new WhiteboardImpl(env, Map2.class.getName());
    vars.set(INT_VAR, INT_VAL);
    vars.set(STRING_VAR, STRING_VAL);
    vars.set(DATE_VAR, DATE_VAL);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map2.class, vars, env);
    //mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRRecord> records = readRecords(env, REAL_OUT_TABLE_NAME_2);
    assertEquals(RECORDS.length, records.size());
    Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS));
    assertTrue(recordsSet.containsAll(records));
    env.delete(MRPath.createFromURI(REAL_IN_TABLE_NAME_2));
    env.delete(MRPath.createFromURI(REAL_OUT_TABLE_NAME_2));
  }

  @MRProcessClass(goal = OUT_TABLE_NAME_3)
  public static final class Map3 {
    final State state;
    public Map3(State state) {
      this.state = state;
    }

    @MRMapMethod(input = IN_TABLE_NAME_3, output = OUT_TABLE_NAME_3)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      for(String i: state.<String[]>get(ARRAY_VAR)) {
        output.add(Integer.parseInt(i), key, sub, value);
      }
    }

  }

  @Test
  public void arraysInNamesShouldWork() {
    writeRecords(env, IN_TABLE_NAME_3, RECORDS);
    Whiteboard vars = new WhiteboardImpl(env, Map3.class.getName());
    vars.set(ARRAY_VAR, ARRAY_VALS);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map3.class, vars, env);
    //mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    for(String i: ARRAY_VALS) {
      final String table = OUT_TABLE_NAME_3.replace("{" + ARRAY_VAR + "}", i);
      List<MRRecord> records = readRecords(env, table);
      assertEquals(RECORDS.length, records.size());
      Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS));
      assertTrue(recordsSet.containsAll(records));
      env.delete(MRPath.createFromURI(table));
    }
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_3));
  }

}
