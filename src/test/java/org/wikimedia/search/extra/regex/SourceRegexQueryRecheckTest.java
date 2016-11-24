package org.wikimedia.search.extra.regex;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.junit.Test;
import org.wikimedia.search.extra.regex.SourceRegexQuery.NonBacktrackingOnTheFlyCaseConvertingRechecker;
import org.wikimedia.search.extra.regex.SourceRegexQuery.NonBacktrackingRechecker;
import org.wikimedia.search.extra.regex.SourceRegexQuery.Rechecker;
import org.wikimedia.search.extra.regex.SourceRegexQuery.Settings;
import org.wikimedia.search.extra.regex.SourceRegexQuery.SlowRechecker;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class SourceRegexQueryRecheckTest {
    private static final Logger log = ESLoggerFactory.getLogger(SourceRegexQueryRecheckTest.class.getPackage().getName());

    private final String rashidun;
    private final String obama;

    public void test() {
        Map<String, Float> fields = new HashMap<>();
        fields.put("plain", 1f);
        fields.put("text", 0.5f);
        
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("ai du").fields(fields).defaultOperator(Operator.AND);
        builder.toQuery()
    }
    public SourceRegexQueryRecheckTest() throws IOException {
        rashidun = Resources.toString(Resources.getResource("Rashidun Caliphate.txt"), Charsets.UTF_8);
        obama = Resources.toString(Resources.getResource("Barack Obama.txt"), Charsets.UTF_8);
    }

    @Test
    public void insensitiveShortRegex() {
        Settings settings = new Settings();
        many("case insensitive", "cat", settings, 1000, false);
    }

    @Test
    public void sensitiveShortRegex() {
        Settings settings = new Settings();
        settings.setCaseSensitive(true);
        many("case sensitive", "cat", settings, 1000, false);
    }

    @Test
    public void insensitiveLongerRegex() {
        Settings settings = new Settings();
        many("case insensitive", "\\[\\[Category:", settings, 1000, true);
    }

    @Test
    public void sensitiveLongerRegex() {
        Settings settings = new Settings();
        settings.setCaseSensitive(true);
        many("case sensitive", "\\[\\[Category:", settings, 1000, true);
    }

    @Test
    public void insensitiveBacktrackyRegex() {
        Settings settings = new Settings();
        settings.setCaseSensitive(true);
        many("case sensitive", "days.+and", settings, 1000, true);
    }

    @Test
    public void sensitiveBacktrackyRegex() {
        Settings settings = new Settings();
        many("case sensitive", "days.+and", settings, 1000, true);
    }

    private void many(String name, String regex, Settings settings, int times, boolean matchIsNearTheEnd) {
        long slow = manyTestCase(new SlowRechecker(regex, settings), "slow", name, settings, times, regex);
        long nonBacktracking = manyTestCase(new NonBacktrackingRechecker(regex, settings), "non backtracking", name, settings, times, regex);
        assertTrue("Nonbacktracking is faster than slow", slow > nonBacktracking);
        if (!settings.isCaseSensitive()) {
            long nonBacktrackingCaseConverting = manyTestCase(new NonBacktrackingOnTheFlyCaseConvertingRechecker(regex, settings),
                    "case converting", name, settings, times, regex);
            if (!matchIsNearTheEnd) {
                assertTrue("On the fly case conversion is faster than up front case conversion",
                        nonBacktracking > nonBacktrackingCaseConverting);
            }
        }
    }

    private long manyTestCase(Rechecker rechecker, String recheckerName, String name, Settings settings, int times, String regex) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            assertTrue(rechecker.recheck(ImmutableList.of(rashidun)));
            assertTrue(rechecker.recheck(ImmutableList.of(obama)));
        }
        long took = System.currentTimeMillis() - start;
        log.info("{} took {} millis to match /{}/", String.format(Locale.ROOT, "%20s %10s", recheckerName, name), took, regex);
        return took;
    }
}
