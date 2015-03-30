package solar.mr;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.StateImpl;
import solar.mr.routines.MRRecord;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRRoutine implements Processor<MRRecord>, Action<CharSequence> {
  public static final String VAR_TIMELIMITPERRECORD = "var:timelimitperrecord";
  private final MRErrorsHandler output;
  public final int MAX_OPERATION_TIME = 600;
  private final State state;
  private final MRPath[] inputTables;
  private final long timeout;
  private int currentInputIndex = 0;
  private boolean interrupted = false;
  private AtomicReference<CharSequence> next = new AtomicReference<>();
  private volatile Throwable unhandled;
  private MRRecord currentRecord;
  private Thread routineTh;

  public MRRoutine(MRPath[] inputTables, MRErrorsHandler output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
    this.timeout = (state != null && state.available(VAR_TIMELIMITPERRECORD)) ? state.<Long>get(VAR_TIMELIMITPERRECORD) : TimeUnit.MINUTES.toMillis(1);

    routineTh = new Thread(new Runnable() {
      @Override
      public void run() {
        CharSequence next;
        do {
          next = MRRoutine.this.next.get();
          if (next != null) {
            invokeInner(next);
            MRRoutine.this.next.set(null);
          }
        }
        while (next != CharSeq.EMPTY && !isStopped);
      }
    });
    routineTh.setDaemon(true);
    routineTh.setName("MRRoutine thread");
    routineTh.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        unhandled = e;
      }
    });
    routineTh.start();
  }

  public MRRoutine(MRPath... inputTables) {
    this(inputTables, new DefaultMRErrorsHandler(), new StateImpl());
  }

  private volatile boolean isStopped = false;
  @Override
  public void invoke(final CharSequence record) {
    if (isStopped)
      return;
    final long time = System.currentTimeMillis();
    int count = 0;

    next.set(record);
    while (unhandled == null && !next.compareAndSet(null, null)) {
      if (++count % 100000 == 0 && System.currentTimeMillis() - time > timeout) {
        System.err.println("time out: " + key);
        unhandled = new TimeoutException();
        /* Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (Thread th:threads) {
          StackTraceElement[] stackTrace = th.getStackTrace();
          System.err.print('\n');
          for(StackTraceElement e : stackTrace) {
            System.err.println(e.getClassName() + ":" + e.getMethodName() + "(" + e.getLineNumber() + ")");
          }
        }
        System.exit(2);
        */
      }
    }
    if (unhandled != null) {
      output.error(unhandled, currentRecord);
      isStopped = true;
      //noinspection deprecation
      routineTh.stop();
    }
  }

  private final CharSequence[] split = new CharSequence[3];
  private void invokeInner(CharSequence record) {
    if (record == CharSeq.EMPTY)
      onEndOfInput();
    if (interrupted || record.length() == 0) // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
      return;
    int parts = CharSeqTools.trySplit(record, '\t', split);
    if (parts == 1) // switch table record
      currentInputIndex = CharSeqTools.parseInt(split[0]);
    else if (parts < 3)
      output.error("Illegal record", "Contains 2 fields only!", new MRRecord(currentTable(), split[0].toString(), "", split[1]));
    else {
      currentRecord = new MRRecord(currentTable(), split[0].toString(), split[1].toString(), split[2]);
      process(currentRecord);
    }
  }

  protected void interrupt() {
    interrupted = true;
  }

  protected void onEndOfInput() {}

  public MRPath currentTable() {
    return inputTables[currentInputIndex];
  }

  public State state() {
    return state;
  }

  public MRPath[] input() {
    return inputTables;
  }

  public MRRecord currentRecord() {
    return currentRecord;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    isStopped = true;
  }
}
