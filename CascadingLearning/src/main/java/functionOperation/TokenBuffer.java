package functionOperation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by sktamang on 10/14/15.
 */
public class TokenBuffer extends BaseOperation implements Buffer {


        static Fields fieldDeclaration = new Fields(
                "document","sentnum", "sentence");
        static Fields fieldOutput = new Fields(
                "documentname","sentnumber", "wordnum", "word","customField","comma");
        public TokenBuffer() {
        super(3, fieldOutput);
    }


    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        int sentnum = 0;
        TupleEntry group = bufferCall.getGroup();
        //get all the current argument values for this grouping
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        /* create a Tuple to hold our result values and set
        its document name field */
        Tuple result = Tuple.size(6);
        int token_count = 0;
        System.out.println("arguments = " + arguments);
        while(arguments.hasNext()){
            Tuple tuple = arguments.next().getTuple();
            String[] tokens =
                    NLPUtils.getTokens(tuple.getString(2));
            for (int i = 0; i < tokens.length; i++){
                String token = tokens[i];
                System.out.println("token = " + token);
                if (token == null || token.isEmpty())
                    continue;
                token_count++;
                result.set(0, group.getString("document"));
                result.set(1, sentnum);
                result.set(2, token_count);
                result.set(3, token);
                result.set(4,"mynewToken");
                result.set(5,"new");
                 // Return the result Tuple
                bufferCall.getOutputCollector().add( result );
                /* See if the last token is an abbreviation.
                * If so, treat the next sentence as a
                * continuation.
                * If not, increment our sentence number
                * and reset the word number.
                */
                if (i == tokens.length - 1) {
                    if (!NLPUtils.isAbbreviation(token))
                    {
                        sentnum++;
                        token_count = 0;
                    }
                }
            } // for
        }// while
    }// operate

}
