package yamr;

import com.spbsu.commons.seq.CharSeqReader;
import com.sun.tools.javac.util.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import solar.mr.DefaultMRErrorsHandler;
import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.MRRoutineBuilder;
import solar.mr.env.LocalMREnv;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRMap;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;

/**
 * User: solar
 * Date: 28.02.15
 * Time: 17:35
 */
public class MRPrimitivesTest {
  private final LocalMREnv localMREnv = LocalMREnv.createTemp();
  @Rule
  public TestName name = new TestName();
  private static final MRPath oneRecordShard = MRPath.create("/tmp/one-record");

  public MRPrimitivesTest() {
    localMREnv.append(oneRecordShard, new CharSeqReader("one\t#\tvalue\n"));
  }

  @Test
  public void testMapExecutionTimeout() {
    final MRRoutineBuilder builder = new MRRoutineBuilder() {
      @Override
      public RoutineType getRoutineType() {
        return RoutineType.MAP;
      }
      @Override
      public MRRoutine build(MROutput output) {
        return new MRMap(input(), output, state) {
          @Override
          public void map(MRPath table, String sub, CharSequence value, String key) {
            //noinspection InfiniteLoopStatement,StatementWithEmptyBody
            while(true);
          }
        };
      }
    };
    runTimeout(builder);
  }

  @Test
  public void testReduceExecutionTimeout() {
    final MRRoutineBuilder builder = new MRRoutineBuilder() {
      @Override
      public RoutineType getRoutineType() {
        return RoutineType.REDUCE;
      }
      @Override
      public MRRoutine build(MROutput output) {
        return new MRReduce(input(), output, state) {
          @Override
          public void reduce(String key, Iterator<MRRecord> reduce) {
            //noinspection InfiniteLoopStatement,StatementWithEmptyBody
            while(true);
          }
        };
      }
    };
    runTimeout(builder);
  }

  private void runTimeout(MRRoutineBuilder builder) {
    final WhiteboardImpl whiteboard = new WhiteboardImpl(localMREnv, name.getMethodName());
    whiteboard.set(MRRoutine.VAR_TIMELIMITPERRECORD, 1000l);
    builder.addInput(oneRecordShard);
    builder.setState(whiteboard.snapshot());
    try {
      localMREnv.execute(builder, new DefaultMRErrorsHandler());
      Assert.check(false);
    }
    catch (RuntimeException re) {
      if (!(re.getCause() instanceof TimeoutException))
        throw re;
    }
  }
}
