package solar.mr.routines;

import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.proc.State;

import java.util.concurrent.*;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MRRoutine {
  public MRMap(final String[] inputTables, final MROutput output, final State state) {
    super(inputTables, output, state);
  }

  @Override
  public final void invoke(final MRRecord rec) {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future future = executor.submit(new Runnable() {
        @Override
        public void run() {
          map(rec.key, rec.sub, rec.value);
        }
      });
      future.get(MAX_OPERATION_TIME, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException("Map is too slow for key: " + rec.key, e);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  public abstract void map(String key, String sub, CharSequence value);
}
