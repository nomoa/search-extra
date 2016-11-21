package org.wikimedia.search.extra.analysis.filters;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.junit.Test;

public class RegexLowerCaseFilterTest extends BaseTokenStreamTestCase {
    @Test
    public void simpleTest() throws IOException {
        Analyzer analyzer = newAnalyzer();
        String input = "HÉLLO GROß WÔRLD";
        try(TokenStream ts = analyzer.tokenStream("", "HÉLLO GROß WÔRLD")) {
            assertTokenStreamContents(ts,
                    new String[]{"héllo groß wôrld"},
                    new int[]{0}, // start offsets
                    new int[]{input.length()}, // end offsets
                    null, // types, not supported
                    new int[]{1}, // pos increments
                    null, // pos size (unsupported)
                    input.length(), // last offset
                    null, //keywordAtts, (unsupported)
                    true);
        }
    }

    @Test
    public void unicodeCompat() {
        // Test that we cover every code points
        for(int i = Character.MIN_CODE_POINT; i <= Character.MAX_CODE_POINT; i++) {
            int expected = Character.toLowerCase(Character.toUpperCase(i));
            int actual = RegexLowerCaseFilter.toLowerCase(i);
            assertEquals(String.format("U+%04x : U+%04x == U+%04x", i, expected, actual), expected, actual);
        }
    }

    private Analyzer newAnalyzer() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tok = new KeywordTokenizer();
                TokenStream ts = new RegexLowerCaseFilter(tok);
                return new TokenStreamComponents(tok, ts);
            }
        };
    }
}
