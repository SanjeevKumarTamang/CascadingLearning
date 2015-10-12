package filterOperation;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by sktamang on 10/12/15.
 */
public class FilterOperationMain {

    public static void main(String[] args) {

// define source and sink Taps.
        Scheme mainScheme = new TextDelimited(
                new Fields("line_num", "line" ), true, "," );
        Tap source = new FileTap( mainScheme, args[0] );
        Tap sink = new FileTap( mainScheme, args[1],
                SinkMode.REPLACE );
        Pipe inPipe = new Pipe("InPipe");
        inPipe = new Each(inPipe, new SampleFilter("test"));
        Flow flow = new LocalFlowConnector().connect(
                source, sink, inPipe );
        flow.complete();
    }
}
