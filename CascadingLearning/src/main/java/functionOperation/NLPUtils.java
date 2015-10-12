package functionOperation;

/**
 * Created by sanjeev on 10/12/15.
 */
public class NLPUtils {
    /* Parse text into sentences
* @param text - document text
* @return String
*/
    public static String[] getSentences(String text)
    {
        String sentences[]=text.split("[.?!]");
        for (int i=0; i< sentences.length; i++)
        {
            sentences[i]=sentences[i].trim();
        }
        return sentences;
    }
}
