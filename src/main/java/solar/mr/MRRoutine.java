package solar.mr;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.State;
import solar.mr.routines.MRRecord;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRRoutine implements Processor<CharSequence>, Action<MRRecord> {

  public final int MAX_OPERATION_TIME = 60;

  protected final MROutput output;
  private final State state;
  private final String[] inputTables;
  private int currentInputIndex = 0;
  private boolean interrupted = false;

  public MRRoutine(String[] inputTables, MROutput output, State state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
  }

  @Override
  public final void process(final CharSequence record) {
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
        invoke(mrRecord);
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

  public String currentTable() {
    return inputTables[currentInputIndex];
  }

  public State state() {
    return state;
  }

  public String[] input() {
    return inputTables;
  }

  public String[] output() {
    return output.names();
  }
}
