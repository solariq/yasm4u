package functional;

import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.WhiteboardImpl;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

/**
 * Created by minamoto on 25/02/15.
 */
@RunWith(Parameterized.class)
public class BigWrites extends BaseMRTest {
    @Test
    public void write32M() {

        Whiteboard wb = new WhiteboardImpl(env, "write32M");
        CharSeqBuilder builder = new CharSeqBuilder();
        builder.append(new char[64 * 1024 * 1024]);
        wb.set("var:b32M", builder.build());
        wb.snapshot();
    }
}
