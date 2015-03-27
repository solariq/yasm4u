package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public abstract class AsteriskTest extends BaseMRTest {

  private final MRRecord[] RECORDS_1 = createRecords(10, 3);
  private final MRRecord[] RECORDS_2 = createRecords(20, 3);
  private final MRRecord[] RECORDS_3 = createRecords(30, 3);

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

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class Map {

    public Map(State state) {
    }

    @MRMapMethod(input = IN_TABLES + "*", output = OUT_TABLE_NAME)
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
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(RECORDS_1.length + RECORDS_2.length + RECORDS_3.length, records.size());
    Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    recordsSet.addAll(Arrays.asList(RECORDS_3));
    assertTrue(recordsSet.containsAll(records));
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_1));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_2));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_3));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
