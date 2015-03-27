package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.*;

import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class MultiReduceTest extends BaseMRTest {

  private final MRRecord[] RECORDS_1 = createRecordsWithKeys(9, 10, "key1", "key2", "key3");
  private final MRRecord[] RECORDS_2 = createRecordsWithKeys(7, 20, "key3", "key4", "key5");

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
      OUT_TABLE_NAME_1
  })
  public static final class Reduce2in1 {

    public Reduce2in1(State state) {
    }

    @MRReduceMethod(input = IN_TABLE_NAME_1, output = OUT_TABLE_NAME_1)
    public void reduce1(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        output.add(record.key, record.sub, record.value);
      }
    }

    @MRReduceMethod(input = IN_TABLE_NAME_2, output = OUT_TABLE_NAME_1)
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
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME_1);
    assertEquals(RECORDS_1.length + RECORDS_2.length, records.size());
    Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS_1));
    recordsSet.addAll(Arrays.asList(RECORDS_2));
    assertTrue(recordsSet.containsAll(records));
  }

  @MRProcessClass(goal = {
      OUT_TABLE_NAME_1,
      OUT_TABLE_NAME_2
  })
  public static final class Reduce2in2 {

    public Reduce2in2(State state) {
    }

    @MRReduceMethod(
        input = {
            IN_TABLE_NAME_1,
            IN_TABLE_NAME_2
        },
        output = {
            OUT_TABLE_NAME_1,
            OUT_TABLE_NAME_2
        })
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        if (record.sub.startsWith("subkey1")) {
          output.add(0, record.key, record.sub, record.value);
        } else if (record.sub.startsWith("subkey2")) {
          output.add(1, record.key, record.sub, record.value);
        } else {
          output.error("Unexpected key", "#", record);
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
      OUT_TABLE_NAME_1,
      OUT_TABLE_NAME_2
  })
  public static final class Reduce1in2 {

    public Reduce1in2(State state) {
    }

    @MRReduceMethod(
        input = {
            IN_TABLE_NAME_1
        },
        output = {
            OUT_TABLE_NAME_1,
            OUT_TABLE_NAME_2
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
    List<MRRecord> records1 = readRecords(env, OUT_TABLE_NAME_1);
    List<MRRecord> records2 = readRecords(env, OUT_TABLE_NAME_2);
    assertEquals(9, records1.size());
    assertEquals(RECORDS_1.length - 9, records2.size());
    for(MRRecord record: records1) {
      if (record.sub.equals("subkey10") || record.sub.equals("subkey11") || record.sub.equals("subkey12")) {
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
