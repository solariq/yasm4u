package solar.mr;

import javax.naming.OperationNotSupportedException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqComposite;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRReduce extends MRRoutine {
  public static final int MAX_REDUCE_SIZE = 100000;
  private final Thread reduceThread;
  private final ArrayBlockingQueue<CharSeq> recordsQueue = new ArrayBlockingQueue<>(MAX_REDUCE_SIZE);

  public MRReduce(final MROutput output) {
    super(output);
    reduceThread = new Thread(new Runnable() {
      CharSeq record;
      @Override
      public void run() {
        try {
          while (true) {
            record = recordsQueue.take();
            final CharSequence[] split = CharSeqTools.split(record, '\t');
            final String key = split[0].toString();
            final Iterator<Pair<String, CharSequence>> reduceIterator = new Iterator<Pair<String, CharSequence>>() {
              @Override
              public boolean hasNext() {
                try {
                  if (record == null && (record = recordsQueue.take()) == CharSeq.EMPTY)
                    return false;
                  return record.subSequence(0, key.length()).equals(key) && record.at(key.length()) == '\t';
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public Pair<String, CharSequence> next() {
                if (!hasNext())
                  throw new NoSuchElementException();
                final CharSeq keyChop = record.subSequence(key.length() + 1);
                int delim = keyChop.indexOf('\t');

                return new Pair<>(keyChop.sub(0, delim).toString(), (CharSequence)keyChop.subSequence(delim + 1));
              }

              @Override
              public void remove() {
                throw new RuntimeException("MR tables are read-only");
              }
            };
            try {
              reduce(key, reduceIterator);
            }
            catch (Exception e) {
              output.error(e, recordsQueue.peek());
            }
            while (reduceIterator.hasNext())
              reduceIterator.next();
          }
        } catch (InterruptedException e) {
          // skip
        }
      }

    }, "Reduce thread");
    reduceThread.setDaemon(true);
  }

  @Override
  public void invoke(final CharSeq record) {
    if (!reduceThread.isAlive() && !reduceThread.isInterrupted())
      reduceThread.start();
    recordsQueue.add(record);
  }

  public abstract void reduce(String key, Iterator<Pair<String, CharSequence>> reduce);
}
