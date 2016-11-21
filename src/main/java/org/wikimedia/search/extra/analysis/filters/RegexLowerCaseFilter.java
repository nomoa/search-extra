package org.wikimedia.search.extra.analysis.filters;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class RegexLowerCaseFilter extends TokenFilter {
    private final CharTermAttribute cattr = addAttribute(CharTermAttribute.class);

    public RegexLowerCaseFilter(TokenStream input) {
        super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            for (int i = 0; i < cattr.length();) {
                int cp = Character.codePointAt(cattr.buffer(), i, cattr.length());
                cp = toLowerCase(cp);
                i += Character.toChars(cp, cattr.buffer(), i);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Should be equivalent to toLowerCase(toUpperCase(cp))
     * @param cp codePoint to lower case
     * @return returns the lowercase version
     */
    public static int toLowerCase(int cp) {
        // In theory Character.toLowerCase(Character.toUpperCase(cp))
        // is equivalent. Having an explicit switch case is twice faster
        // and make this filter almost as fast as the standard LowerCaseFilter.
        // That's maybe a micro-optimization but it sounds interesting
        // to have an explicit list here.
        cp = Character.toLowerCase(cp);
        switch(cp) {
        // GREEK SMALL LETTER MU μ => MICRO SIGN μ
        case '\u00b5': return '\u03bc';
        // LATIN SMALL LETTER DOTLESS I (ı) to I LATIN SMALL LETTER I (i)
        case '\u0131': return '\u0069';
        // LATIN SMALL LETTER LONG S (ſ) to LATIN SMALL LETTER S (s)
        case '\u017f': return '\u0073';
        // COMBINING GREEK YPOGEGRAMMENI to GREEK SMALL LETTER IOTA
        case '\u0345': return '\u03b9';
        // GREEK SMALL LETTER FINAL SIGMA (ς) to GREEK SMALL LETTER SIGMA (σ)
        case '\u03c2': return '\u03c3';
        // GREEK BETA SYMBOL (ϐ) to GREEK SMALL LETTER BETA (β)
        case '\u03d0': return '\u03b2';
        // GREEK THETA SYMBOL (ϑ) to GREEK SMALL LETTER THETA (θ)
        case '\u03d1': return '\u03b8';
        // GREEK PHI SYMBOL (ϕ) to GREEK SMALL LETTER PHI (φ)
        case '\u03d5': return '\u03c6';
        // GREEK PI SYMBOL (ϖ) to GREEK SMALL LETTER PI (π)
        case '\u03d6': return '\u03c0';
        // GREEK KAPPA SYMBOL (ϰ) to GREEK SMALL LETTER KAPPA (κ)
        case '\u03f0': return '\u03ba';
        // GREEK RHO SYMBOL (ϱ) to GREEK SMALL LETTER RHO (ρ)
        case '\u03f1': return '\u03c1';
        // GREEK LUNATE EPSILON SYMBOL (ϵ) to GREEK SMALL LETTER EPSILON (ε)
        case '\u03f5': return '\u03b5';
        // LATIN SMALL LETTER LONG S WITH (ẛ) to LATIN SMALL LETTER S WITH DOT (ṡ)
        case '\u1e9b': return '\u1e61';
        // GREEK PROSGEGRAMMENI (ι) to GREEK SMALL LETTER IOTA (ι)
        case '\u1fbe': return '\u03b9';
        default: return cp;
        }
    }
}
