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

import static solar.mr.MRTestUtils.*;

/**
 * Created by inikifor on 05.12.14.
 */
@RunWith(Parameterized.class)
public final class EmptinessTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(10);

  private static final String IN_TABLE_NAME_EMPTY = TABLE_NAME_PREFIX + "EmptinessTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "EmptinessTest-2-" + SALT;
  private static final String IN_TABLE_NAME_FULL = TABLE_NAME_PREFIX + "EmptinessTest-3-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME_FULL, RECORDS);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class MapEmptyInput {

    public MapEmptyInput(State state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_EMPTY, output = SCHEMA + OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      output.add(key, sub, value);
    }

  }

  @Test
  public void emptyInputTableShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapEmptyInput.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class MapEmptyOutput {

    public MapEmptyOutput(State state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME_FULL, output = SCHEMA + OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
//      System.err.println("Hello!");
      // do nothing
    }

  }

  @Test
  public void emptyOutputTableShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapEmptyOutput.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_FULL));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
