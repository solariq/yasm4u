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
    System.err.println("MAP: user :" + state.get("var:mr.user"));
  }

  @Override
  public final void invoke(final MRRecord rec) {
    map(rec.key, rec.sub, rec.value);
    /*final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future future = executor.submit(new Runnable() {
        @Override
        public void run() {
          map(rec.key, rec.sub, rec.value);
        }
      });
      future.get(MAX_OPERATION_TIME, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      System.err.println("Map is too slow for key: " + rec.key + "subkey: " + rec.sub);
      throw new RuntimeException("Map is too slow for key: " + rec.key, e);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdown();
    }*/

    /*final Thread worker = new Thread(new Runnable() {
      @Override
      public void run() {
        map(rec.key, rec.sub, rec.value);
      }
    });
    worker.start();
    try {
      worker.join(MAX_OPERATION_TIME * 1000);
    } catch (Exception e) {

      System.err.println("Map too slow key: " + rec.key + " subkey: " + rec.sub);
      System.err.flush();
      worker.interrupt();
      //throw new RuntimeException(e);
    }*/
  }

  public abstract void map(String key, String sub, CharSequence value);
}
