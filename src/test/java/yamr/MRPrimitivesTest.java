package yamr;

import com.spbsu.commons.util.logging.Interval;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;
import ru.yandex.se.yasm4u.domains.mr.ops.MRMap;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.MRReduce;

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
      public MROperation build(MROutput output) {
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
      public MROperation build(MROutput output) {
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
      public MROperation build(MROutput output) {
        return new MRMap(input(), output, state) {
          @Override
          public void map(MRPath table, String sub, CharSequence value, String key) {
            counter++;
          }
        };
      }
    };
    final WhiteboardImpl whiteboard = new WhiteboardImpl(fakeMREnv, name.getMethodName());
    whiteboard.set(MROperation.VAR_TIMELIMITPERRECORD, 1000l);
    builder.addInput(FakeMREnv.oneMillionRecordShard);
    builder.setState(whiteboard.snapshot());
    try {
      System.gc();
      Interval.start();
      fakeMREnv.execute(builder, new DefaultMRErrorsHandler());
      Interval.stopAndPrint();
      if (Interval.time() >= 2000)
        throw new RuntimeException("interval >= 2000");
    }
    catch (RuntimeException re) {
      re.printStackTrace();
      throw re;
    }
  }

  private void runTimeout(MRRoutineBuilder builder) {
    final WhiteboardImpl whiteboard = new WhiteboardImpl(fakeMREnv, name.getMethodName());
    whiteboard.set(MROperation.VAR_TIMELIMITPERRECORD, 1000l);
    builder.addInput(FakeMREnv.oneRecordShard);
    builder.setState(whiteboard.snapshot());
    try {
      fakeMREnv.execute(builder, new DefaultMRErrorsHandler() {
        @Override
        public void error(Throwable th, MRRecord record) {
          if (!"0".equals(record.key)){
            throw new RuntimeException("unexpected key: " + record.key);
          }
          super.error(th, record);
        }
      });
      throw new RuntimeException("unexpected");
    }
    catch (RuntimeException re) {
      if (!(re.getCause() instanceof TimeoutException))
        throw re;
    }
  }
}
