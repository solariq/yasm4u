package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.io.*;

/**
* User: solar
* Date: 17.10.14
* Time: 10:36
*/
public abstract class MROutputBase implements MROutput {
  private final MRPath[] outTables;
  private final int errorTable;
  private int lastActiveTable = 0;

  protected MROutputBase(MRPath[] outTables) {
    this.outTables = outTables;
    this.errorTable = outTables.length;
  }

  @Override
  public void add(final String key, final String subkey, final CharSequence value) {
    add(0, key, subkey, value);
  }

  @Override
  public void add(final int tableNo, final String key, final String subkey, final CharSequence value) {
    if (tableNo > errorTable)
      throw new IllegalArgumentException("Incorrect table index: " + tableNo);
    if (tableNo == errorTable)
      throw new IllegalArgumentException("Errors table #" + tableNo + " must be accessed via error subroutines of outputter");
    if (key.isEmpty())
      throw new IllegalArgumentException("Key must be non empty!");
    if (key.getBytes().length > 4096)
      throw new IllegalArgumentException("Key must not exceed 4096 byte length!");
    if (subkey.isEmpty())
      throw new IllegalArgumentException("Subkey must be non empty");
    if (subkey.getBytes().length > 4096)
      throw new IllegalArgumentException("Subkey must not exceed 4096 byte length!");
    if (CharSeqTools.indexOf(value, "\n") >= 0)
      throw new IllegalArgumentException("Value can not contain \\n symbols for stream usage");

    push(tableNo, CharSeqTools.concatWithDelimeter("\t", key, subkey, value));
  }

  @Override
  public MRPath[] names() {
    return outTables;
  }

  @Override
  public void error(final Throwable th, final MRRecord rec) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final ObjectOutputStream dos = new ObjectOutputStream(out)) {
      dos.writeObject(th);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    error(th.getClass().getName(), CharSeqTools.toBase64(out.toByteArray()).toString(), rec);
  }

  private int counter = 0;
  @Override
  public void error(final String type, final String cause, final MRRecord rec) {
    counter++;
    push(errorTable, CharSeqTools.concatWithDelimeter("\t", type, encodeKey(cause),
        rec.source != null ? rec.source.toString() : "null", rec.toString()));
  }

  @Override
  public int errorsCount() {
    return counter;
  }

  private CharSequence encodeKey(final String cause) {
    return CharSeqTools.replace(CharSeqTools.replace(cause, "\n", "\\n"), "\t", "\\t");
  }

  public void parse(CharSequence arg) {
    if (arg.length() == 0)
      return;
    final CharSequence[] split = CharSeqTools.split(arg, '\t');

    if (split.length == 1) {
      lastActiveTable = CharSeqTools.parseInt(split[0]);
    }
    else if (split.length >= 3) {
      if (lastActiveTable == errorTable) {
        final MRRecord rec = new MRRecord(MRPath.create(split[2].toString()), split[3].toString(), split[4].toString(), split[5]);

        boolean isException = false;
        {
          final Class<?> aClass;
          try {
            aClass = Class.forName(split[0].toString());
            isException = Throwable.class.isAssignableFrom(aClass);
          } catch (ClassNotFoundException e) {
            //
          }
        }
        if (isException) {
          try (final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(CharSeqTools.parseBase64(split[1])))) {
            error((Throwable) is.readObject(), rec);
          } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
        else {
          error(split[0].toString(), split[1].toString(), rec);
        }
      }
      else push(lastActiveTable, arg);
    }
    else throw new IllegalArgumentException("Can not parse MRRecord from string: " + arg);
  }

  protected abstract void push(int tableNo, CharSequence record);
  public abstract void stop();
  public abstract void interrupt();
  public abstract void join();
}
