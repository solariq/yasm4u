package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.MRUtils;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.routines.MRRecord;

import java.util.Iterator;

import static solar.mr.MRUtils.createRecords;
import static solar.mr.MRUtils.dropMRTable;
import static solar.mr.MRUtils.writeRecords;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class ErrorsTest extends BaseMRTest {

  private final MRUtils.Record[] RECORDS = createRecords(1);

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "ErrorsTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "ErrorsTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class MapError {

    public MapError(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
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

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class MapException {

    public MapException(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      throw new RuntimeException();
    }

  }

  @Test
  public void exceptionInMapShouldThrowException() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapError.class, env);
    mrProcess.wb().wipe();
    try {
      mrProcess.execute();
      assertTrue(false);
    } catch (RuntimeException ignore) {
    }
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class ReduceError {

    public ReduceError(MRState state) {
    }

    @MRReduceMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
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

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class ReduceException {

    public ReduceException(MRState state) {
    }

    @MRReduceMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
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

  @MRProcessClass(goal = SCHEMA + IN_TABLE_NAME)
  public static final class MapIgnored {

    public MapIgnored(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
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

  @After
  public void dropTable() {
    dropMRTable(env, IN_TABLE_NAME);
    dropMRTable(env, OUT_TABLE_NAME);
  }

}
