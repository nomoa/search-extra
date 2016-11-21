package org.wikimedia.search.extra.analysis.filters;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

public class RegexNGramAnalyzer extends Analyzer {
    public final static int DEFAULT_NGRAM_SIZE = 3;
    private final int nGramSize;

    /**
     * Build a new RegexNGramAnalyzer suited to be use
     */
    public RegexNGramAnalyzer() {
        this(DEFAULT_NGRAM_SIZE);
    }

    /**
     * @param nGramSize the size of generated ngrams
     */
    public RegexNGramAnalyzer(int nGramSize) {
        super();
        this.nGramSize = nGramSize;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tok = new NGramTokenizer(nGramSize, nGramSize);
        TokenStream ts = new RegexLowerCaseFilter(tok);
        ts = new ASCIIFoldingFilter(ts);
        return new TokenStreamComponents(tok, ts);
    }

    /**
     * @return the size of generated ngrams
     */
    public int getnGramSize() {
        return nGramSize;
    }
}
