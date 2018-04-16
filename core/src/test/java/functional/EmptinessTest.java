package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import static com.expleague.yasm4u.domains.mr.MRTestUtils.createRecords;
import static com.expleague.yasm4u.domains.mr.MRTestUtils.writeRecords;

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

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapEmptyInput {

    public MapEmptyInput(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME_EMPTY, output = OUT_TABLE_NAME)
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

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapEmptyOutput {

    public MapEmptyOutput(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME_FULL, output = OUT_TABLE_NAME)
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
