package solar.mr;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.StateImpl;
import solar.mr.routines.MRRecord;

import java.util.Timer;
import java.util.TimerTask;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRRoutine implements Processor<MRRecord>, Action<CharSequence> {
  private final MRErrorsHandler output;
  public final int MAX_OPERATION_TIME = 600;
  private final State state;
  private final MRPath[] inputTables;
  private int currentInputIndex = 0;
  private boolean interrupted = false;

  public MRRoutine(MRPath[] inputTables, MRErrorsHandler output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
  }

  public MRRoutine(MRPath... inputTables) {
    this(inputTables, new MRErrorsHandler() {
      @Override
      public void error(String type, String cause, MRRecord record) {
        throw new RuntimeException("Error during record processing! type: " + type + ", cause: " + cause + ", record: [" + record.toString() + "]");
      }

      @Override
      public void error(Throwable th, MRRecord record) {
        if (th instanceof RuntimeException)
          throw (RuntimeException)th;
        throw new RuntimeException(th);
      }

      @Override
      public int errorsCount() {
        return 0;
      }
    }, new StateImpl());
  }

  @Override
  public void invoke(final CharSequence record) {
    if (record == CharSeq.EMPTY)
      onEndOfInput();
    if (interrupted || record.length() == 0) // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
      return;
    final CharSequence[] split = new CharSequence[3];
    int parts = CharSeqTools.trySplit(record, '\t', split);
    if (parts == 1) // switch table record
      currentInputIndex = CharSeqTools.parseInt(split[0]);
    else if (parts < 3)
      output.error("Illegal record", "Contains 2 fields only!", new MRRecord(currentTable(), split[0].toString(), "", split[1]));
    else {
      final MRRecord mrRecord = new MRRecord(currentTable(), split[0].toString(), split[1].toString(), split[2]);
      try {
        process(mrRecord);
      }
      catch (Exception e) {
        output.error(e, mrRecord);
        interrupt();
      }
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
}
