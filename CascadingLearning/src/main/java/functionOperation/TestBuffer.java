package functionOperation;
import cascading.flow.Flow;
//import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
// import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
// import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import functionOperation.SentFunc;
import functionOperation.TokenBuffer;

/**
 * Created by sktamang on 10/14/15.
 */
public class TestBuffer {
    public static void main(String[] args) {

        Fields fieldDeclarationInput = new Fields("document","text");
        Fields fieldDeclarationOutput = new Fields("documentname","sentnumber","wordnum", "word","customField","comma");

        Scheme inputScheme = new TextDelimited(fieldDeclarationInput,true,",");
        Scheme outputScheme = new TextDelimited(fieldDeclarationOutput,true,",");

        String inputFile="functionBufferInput";
        String outputFile="functionBufferOuput";

        Tap docTap = new FileTap (inputScheme, inputFile,SinkMode.REPLACE);
        Tap sinkTap = new FileTap(outputScheme, outputFile, SinkMode.REPLACE );

        Pipe inPipe = new Pipe("InPipe");
        inPipe= new Each(inPipe, new SentFunc());
        inPipe = new GroupBy(inPipe, new Fields("document"), new Fields("sentnum"));
        inPipe = new Every(inPipe, new TokenBuffer(),fieldDeclarationOutput);
        /* For Hadoop -initialize app properties, tell Hadoop which
        *jar file to use
        *
        * Properties properties = new Properties();
        * AppProps.setApplicationJarClass( properties,
        *
        TestBuffer.class );3
        *
        * FlowConnector flowConnector = new HadoopFlowConnector(
        *
        properties);
        * Flow flow = flowConnector.connect( "process", docTap,
        *
        sinkTap, inPipe);
        */
        // Local flow connector
        Flow flow = new LocalFlowConnector().connect( "process", docTap,
                sinkTap, inPipe );
        System.out.println("flow made");
        // execute the flow, block until complete
        flow.complete();
    }
}
