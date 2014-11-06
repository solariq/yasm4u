package solar.mr;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.MRState;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRRoutine implements Processor<CharSequence>, Action<MRRoutine.Record> {
  protected final MROutput output;
  private final MRState state;
  private final String[] inputTables;
  private int currentInputIndex = 0;
  private boolean interrupted = false;

  public MRRoutine(String[] inputTables, MROutput output, MRState state) {
    this.inputTables = inputTables;
    this.output = output;
    this.state = state;
  }

  @Override
  public final void process(final CharSequence record) {
    if (record == CharSeq.EMPTY)
      onEndOfInput();
    if (record.length() == 0)
      return;

    if (interrupted) // this is trash and ugar but we need to read entire stream before closing it, so that YaMR won't gone mad
      return;
    try {
      final CharSequence[] split = CharSeqTools.split(record, '\t');
      if (split.length == 1) // switch table record
        currentInputIndex = CharSeqTools.parseInt(split[0]);
      else if (split.length < 3)
        output.error("Illegal record", "Contains less then 3 fields", inputTables[currentInputIndex], record);
      else
        invoke(new Record(currentTable(), split[0].toString(), split[1].toString(), record.subSequence(
            split[0].length() + split[1].length() + 2, record.length())));
    }
    catch (Exception e) {
      output.error(e, currentTable(), record);
      interrupt();
    }
  }

  protected void interrupt() {
    interrupted = true;
  }

  protected void onEndOfInput() {}

  public String currentTable() {
    return inputTables[currentInputIndex];
  }

  public MRState state() {
    return state;
  }

  public static class Record {
    public String source;
    public String key;
    public String sub;
    public CharSequence value;

    public Record(final String source, final String key, final String sub, final CharSequence value) {
      this.key = key;
      this.sub = sub;
      this.value = value;
    }

    @Override
    public String toString() {
      return key + "\t" + sub + "\t" + value.toString();
    }
  }
}
