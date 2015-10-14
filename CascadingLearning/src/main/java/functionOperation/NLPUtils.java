package functionOperation;

import java.util.HashSet;

/**
 * Created by sanjeev on 10/12/15.
 */
public class NLPUtils {
    /* Parse text into sentences
* @param text - document text
* @return String
*/
    public static String[] getSentences(String text) {
        System.out.println("text = " + text);
        String sentences[] = text.split("[.?!]");
        for (int i = 0; i < sentences.length; i++) {
            sentences[i] = sentences[i].trim();
        }
        return sentences;
    }

    public static String[] getTokens(String sentence) {
        return sentence.split("[ ,;:]");
    }


    //to handle the cases like Mr. or Mrs. not to mean the end of the sentence
    static HashSet<String> abbrevs = new HashSet<String>();

    static {
        loadTestData();
    }

    private static void loadTestData() {
        abbrevs.add("MR");
        abbrevs.add("MRS");
        abbrevs.add("MS");
        abbrevs.add("DR");
        abbrevs.add("PHD");
        abbrevs.add("ST");
        abbrevs.add("AVE");
        abbrevs.add("RD");
    }


//    method to check whether the token (or word) in
//    question is indeed an abbreviation. It will return true if it is one of the words
//    in our abbrevs list:
    public static boolean isAbbreviation(String token)
    {
        token = token.toUpperCase();
        if (abbrevs.contains(token))
            return true;
        return false;
    }


}
