package implementations;

import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by sanjeev on 10/13/15.
 */
public class ImplementationMain {

    public static void main(String[] args){

        Scheme sourceScheme=new TextDelimited(new Fields(),true,",");
        Tap sourceTap=new FileTap(sourceScheme,args[0]);

        Scheme sinkScheme=new TextDelimited(new Fields(),true,",");
        Tap sinkTap=new FileTap(sinkScheme,args[1], SinkMode.REPLACE);

        Pipe filter=new Pipe("filter");
    }
}
