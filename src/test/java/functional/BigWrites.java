package functional;

import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.WhiteboardImpl;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by minamoto on 25/02/15.
 */
@RunWith(Parameterized.class)
public class BigWrites extends BaseMRTest {
    @Ignore
    @Test
    public void write32M() {

        Whiteboard wb = new WhiteboardImpl(env, "write32M");
        CharSeqBuilder builder = new CharSeqBuilder();
        char[] array = new char[32 * 1024 * 1024];
        Arrays.fill(array, 'o');
        builder.append(array);
        wb.set("var:timelimitperrecord", 6 * 60 * 1000);
        wb.set("var:b32M", builder.build());
        wb.snapshot();
    }
}
