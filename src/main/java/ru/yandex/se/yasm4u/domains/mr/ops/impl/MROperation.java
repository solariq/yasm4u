package ru.yandex.se.yasm4u.domains.mr.ops.impl;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import ru.yandex.se.yasm4u.domains.mr.MRErrorsHandler;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.impl.StateImpl;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MROperation implements Processor<MRRecord>, Action<CharSequence> {
  public static final String VAR_TIMELIMITPERRECORD = "var:timelimitperrecord";
  private final MRErrorsHandler output;
  public final int MAX_OPERATION_TIME = 600;
  private final State state;
  private final MRPath[] inputTables;
  private final long timeout;
  private int currentInputIndex = 0;
  private volatile boolean interrupted = false;
  private final AtomicReference<CharSequence> next = new AtomicReference<>();
  private volatile Throwable unhandled;
  private volatile MRRecord currentRecord;
  private final Thread routineTh;

  public MROperation(MRPath[] inputTables, MRErrorsHandler output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
    final Long timeLimit = state != null ? state.<Long>get(VAR_TIMELIMITPERRECORD) : null;
    this.timeout = timeLimit != null ? timeLimit : TimeUnit.MINUTES.toMillis(5);

    routineTh = new Thread(new Runnable() {
      @Override
      public void run() {
        CharSequence next;
        do {
          next = MROperation.this.next.get();
          if (next != null) {
            invokeInner(next);
            MROperation.this.next.set(null);
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
        System.err.println("---8<---");
        e.printStackTrace(System.err);
        System.err.println("---8<---");
      }
    });
    routineTh.start();
  }

  public MROperation(MRPath... inputTables) {
    this(inputTables, new DefaultMRErrorsHandler(), new StateImpl());
  }

  private volatile boolean isStopped = false;
  @Override
  public void invoke(final CharSequence record) {
    if (isStopped) {
      dumpThread("stopped");
      return;
    }
    final long time = System.currentTimeMillis();
    int count = 0;

    next.set(record);
    while (unhandled == null && !next.compareAndSet(null, null)) {
      if (++count % 100000 == 0) {
        if (System.currentTimeMillis() - time > timeout) {
          // unhandled = new TimeoutException();
          System.err.println("TIMEOUT OCCURS");
          Thread[] threads = new Thread[Thread.activeCount()];
          Thread.enumerate(threads);
          for (Thread th : threads) {
            System.err.println("\nthread: " + th.toString());
            StackTraceElement[] stackTrace = th.getStackTrace();
            for (StackTraceElement e : stackTrace) {
              System.err.println("at " + e.getClassName() + "." + e.getMethodName()
                      + "(" + e.getFileName() + ":" + e.getLineNumber() + ")");
            }
          }
          System.exit(2);
        }
        /**
         * This operation provoke operation hang. please envisitgate it deeper, and then use.
         * else LockSupport.parkNanos(100000);
         */
      }
    }
    if (unhandled != null) {
      output.error(unhandled, currentRecord);
      isStopped = true;
      onEndOfInput();
      //noinspection deprecation
      routineTh.stop();
    }
  }

  private final CharSequence[] split = new CharSequence[3];
  private void invokeInner(CharSequence record) {
    if (record == CharSeq.EMPTY) { // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
      dumpThread("EMPTY");
      onEndOfInput();
      return;
    }
    if (interrupted || record.length() == 0) {
      dumpThread("interrupted: " + interrupted + "record.length: "  + record.length());
      return;
    }
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

  public static void dumpThread(String msg) {
    final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    final String threadName = Thread.currentThread().getName();
    System.err.println(threadName + ": "+ stackTrace[1].getMethodName() + ":" + stackTrace[1].getLineNumber() + ": " + msg);
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
