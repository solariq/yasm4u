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
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class ErrorsTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(1);

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "ErrorsTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "ErrorsTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapError {

    public MapError(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      output.error("Error!", "Error!", new MRRecord(null, key, sub, value));
    }

  }

  @Test
  public void errorOutputInMapShouldThrowException() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapError.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException ignore) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapException {

    public MapException(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      throw new RuntimeException();
    }

  }

  @Test
  public void exceptionInMapShouldThrowException() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapException.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException ignore) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class ReduceError {

    public ReduceError(State state) {
    }

    @MRReduceMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      output.error("Error!", "Error!", new MRRecord(null, key, "#", "#"));
    }

  }

  @Test
  public void errorOutputInReduceShouldThrowException() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(ReduceError.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException ignore) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class ReduceException {

    public ReduceException(State state) {
    }

    @MRReduceMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      throw new RuntimeException();
    }

  }

  @Test
  public void exceptionInReduceShouldThrowException() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(ReduceException.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException ignore) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = IN_TABLE_NAME)
  public static final class MapIgnored {

    public MapIgnored(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      output.error("Error!", "Error!", new MRRecord(null, key, sub, value));
    }

  }

  @Test
  public void nothingShouldHappenIfGoalIsReached() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapIgnored.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
    } catch (RuntimeException e) {
      assertTrue(false);
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapEndless {

    public MapEndless(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      long i = 0;
      while (true) {
        i++;
        if (false) {
          break;
        }
      }
      output.add(key, sub, String.valueOf(i));;
    }
  }

  //2@Test
  public void endlessMapShouldBeKilledByForce() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapEndless.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException expected) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class ReduceEndless {

    public ReduceEndless(State state) {
    }

    @MRReduceMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      reduce.next();
      long i = 0;
      while (true) {
        i++;
        if (false) {
          break;
        }
      }
      output.add(key, "#", String.valueOf(i));;
    }
  }

  //@Test
  public void endlessReduceShouldBeKilledByForce() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(ReduceEndless.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException expected) {
    }
    mrProcess.wb().wipe();
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
