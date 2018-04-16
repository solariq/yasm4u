package com.expleague.yasm4u.domains.mr.ops.impl;

import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.impl.StateImpl;
import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MROperation implements Consumer<MRRecord> {
  public static final String VAR_TIMELIMITPERRECORD = "var:timelimitperrecord";
  private final MRErrorsHandler output;
  public final int MAX_OPERATION_TIME = 600;
  private final State state;
  private final MRPath[] inputTables;
  private final RecordParser parser;

  public MROperation(MRPath[] inputTables, MRErrorsHandler output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
    final Long timeLimit = state != null ? state.<Long>get(VAR_TIMELIMITPERRECORD) : null;
    parser = new RecordParser(timeLimit != null ? timeLimit : TimeUnit.MINUTES.toMillis(5));
  }

  public MROperation(MRPath... inputTables) {
    this(inputTables, new DefaultMRErrorsHandler(), new StateImpl());
  }

  private volatile boolean isStopped = false;

  protected void interrupt() {
    parser.interrupted = true;
  }

  protected void onEndOfInput() {}

  @SuppressWarnings("WeakerAccess")
  public MRPath currentTable() {
    return inputTables[parser.currentInputIndex];
  }

  public State state() {
    return state;
  }

  public MRPath[] input() {
    return inputTables;
  }

  public MRRecord currentRecord() {
    return parser.currentRecord;
  }

  public Consumer<CharSequence> recordParser() {
    return this.parser;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    isStopped = true;
  }

  private class RecordParser implements Consumer<CharSequence> {
    private final long timeout;
    private volatile int currentInputIndex = 0;
    private volatile boolean interrupted = false;
    private final AtomicReference<CharSequence> next = new AtomicReference<>();
    private volatile Throwable unhandled;
    private volatile MRRecord currentRecord;
    private final Thread routineTh;

    private RecordParser(long timeout) {
      this.timeout = timeout;
      routineTh = new Thread(() -> {
        CharSequence next;
        do {
          next = RecordParser.this.next.get();
          if (next != null) {
            invokeInner(next);
            RecordParser.this.next.set(null);
          }
        }
        while (next != CharSeq.EMPTY && !isStopped);
      });
      routineTh.setDaemon(true);
      routineTh.setName("MRRoutine thread");
      routineTh.setUncaughtExceptionHandler((t, e) -> {
        unhandled = e;
        System.err.println("---8<---");
        e.printStackTrace(System.err);
        System.err.println("---8<---");
      });
      routineTh.start();
    }

    @Override
    public void accept(final CharSequence record) {
      if (isStopped)
        return;
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
        /*
         * This operation provoke operation hang. please investigate it deeper, and then use.
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

    /* this function called in thread routine thread */
    private void invokeInner(CharSequence record) {
      if (record == CharSeq.EMPTY || interrupted || record.length() == 0) { // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
        onEndOfInput();
        return;
      }
      int parts = CharSeqTools.trySplit(record, '\t', split);
      if (parts == 1) // switch table record
        currentInputIndex = CharSeqTools.parseInt(split[0]);
      else if (parts < 3)
        output.error("Illegal record", "Contains 2 fields only!", new MRRecord(currentTable(), split[0].toString(), "", split[1]));
      else {
        currentRecord = new MRRecord(currentTable(), split[0].toString(), split[1].toString(), split[2]);
        MROperation.this.accept(currentRecord);
      }
    }
  }
}
