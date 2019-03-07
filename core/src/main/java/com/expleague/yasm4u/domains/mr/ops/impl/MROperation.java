package com.expleague.yasm4u.domains.mr.ops.impl;

import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.env.MROutputBase;
import com.expleague.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.impl.StateImpl;
import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MROperation implements Consumer<MRRecord>, Serializable {
  public static final String VAR_TIMELIMITPERRECORD = "var:timelimitperrecord";
  public static final int MAX_OPERATION_TIME = 600;

  private final State state;
  private final MRPath[] inputTables;
  private final MRErrorsHandler output;

  private transient final AtomicReference<MRRecord> next = new AtomicReference<>();
  private transient volatile boolean interrupted = false;

  private transient final RecordParser parser;

  public MROperation(MRPath[] inputTables, MRErrorsHandler output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
    final Long timeLimit = state != null ? state.<Long>get(VAR_TIMELIMITPERRECORD) : null;
    parser = new RecordParser(timeLimit != null ? timeLimit : TimeUnit.MINUTES.toMillis(5));
    final Thread routineTh = new Thread(() -> {
      MRRecord nextRecord;
      do {
        nextRecord = this.next.get();
        if (nextRecord != null) {
          accept(nextRecord);
          this.next.set(null);
        }
      }
      while (!isStopped);
    });
    routineTh.setDaemon(true);
    routineTh.setName("MRRoutine thread");
    routineTh.setUncaughtExceptionHandler((t, e) -> {
      isStopped = true;
      output.error(e, this.next.get());
    });
    routineTh.start();
  }

  public MROperation(MRPath... inputTables) {
    this(inputTables, new DefaultMRErrorsHandler(), new StateImpl());
  }

  private volatile boolean isStopped = false;

  protected void interrupt() {
    interrupted = true;
  }

  protected void onEndOfInput() {
    if (output instanceof MROutputBase)
      ((MROutputBase) output).stop();
  }

  public State state() {
    return state;
  }

  public MRPath[] input() {
    return inputTables;
  }

  public MRRecord currentRecord() {
    return next.get();
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

    private RecordParser(long timeout) {
      this.timeout = timeout;
    }

    private final CharSequence[] split = new CharSequence[3];
    @Override
    public void accept(final CharSequence record) {
      if (isStopped)
        return;
      final long time = System.currentTimeMillis();
      int count = 0;
      if (record == CharSeq.EMPTY || interrupted || record.length() == 0) // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
        isStopped = true;
      else {
        final int parts = CharSeqTools.trySplit(record, '\t', split);
        switch (parts) {
          case 1: // switch table record
            currentInputIndex = CharSeqTools.parseInt(split[0]);
            break;
          case 2:
            output.error("Illegal record", "Contains 2 fields only!", new MRRecord(currentTable(), split[0].toString(), "", split[1]));
            break;
          default: {
            next.set(new MRRecord(currentTable(), split[0].toString(), split[1].toString(), split[2]));
            while (!isStopped && !next.compareAndSet(null, null)) {
              if (++count % 1000 == 0) {
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
                try {
                  Thread.sleep(10);
                }
                catch (InterruptedException ignore) {}
              }
              Thread.yield();
            }
          }
        }
      }
      if (isStopped) {
        onEndOfInput();
      }
    }

    @SuppressWarnings("WeakerAccess")
    public MRPath currentTable() {
      return inputTables[parser.currentInputIndex];
    }
  }
}
