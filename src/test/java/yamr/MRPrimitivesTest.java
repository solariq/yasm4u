package yamr;

import com.spbsu.commons.util.logging.Interval;
import com.sun.tools.javac.util.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import solar.mr.DefaultMRErrorsHandler;
import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.MRRoutineBuilder;
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
  private final FakeMREnv fakeMREnv = new FakeMREnv();
  @Rule
  public TestName name = new TestName();

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

  @Test
  public void testMapPerformance() {
    final MRRoutineBuilder builder = new MRRoutineBuilder() {
      int counter = 0;
      @Override
      public RoutineType getRoutineType() {
        return RoutineType.MAP;
      }
      @Override
      public MRRoutine build(MROutput output) {
        return new MRMap(input(), output, state) {
          @Override
          public void map(MRPath table, String sub, CharSequence value, String key) {
            counter++;
          }
        };
      }
    };
    final WhiteboardImpl whiteboard = new WhiteboardImpl(fakeMREnv, name.getMethodName());
    whiteboard.set(MRRoutine.VAR_TIMELIMITPERRECORD, 1000l);
    builder.addInput(FakeMREnv.oneMillionRecordShard);
    builder.setState(whiteboard.snapshot());
    try {
      System.gc();
      Interval.start();
      fakeMREnv.execute(builder, new DefaultMRErrorsHandler());
      Interval.stopAndPrint();
      Assert.check(Interval.time() < 2000);
    }
    catch (RuntimeException re) {
      re.printStackTrace();
      Assert.check(false);
    }
  }

  private void runTimeout(MRRoutineBuilder builder) {
    final WhiteboardImpl whiteboard = new WhiteboardImpl(fakeMREnv, name.getMethodName());
    whiteboard.set(MRRoutine.VAR_TIMELIMITPERRECORD, 1000l);
    builder.addInput(FakeMREnv.oneRecordShard);
    builder.setState(whiteboard.snapshot());
    try {
      fakeMREnv.execute(builder, new DefaultMRErrorsHandler() {
        @Override
        public void error(Throwable th, MRRecord record) {
          Assert.check("0".equals(record.key));
          super.error(th, record);
        }
      });
      Assert.check(false);
    }
    catch (RuntimeException re) {
      if (!(re.getCause() instanceof TimeoutException))
        throw re;
    }
  }
}
