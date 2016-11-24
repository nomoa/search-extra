package org.wikimedia.search.extra.regex;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.wikimedia.search.extra.regex.expression.ExpressionRewriter;
import org.wikimedia.search.extra.util.FieldValues;

/**
 * Builds source_regex filters.
 */
public class SourceRegexQueryBuilder extends AbstractQueryBuilder<SourceRegexQueryBuilder> {
    public static final String NAME = "source_regex";
    public static final ParseField NAME_FIELD = new ParseField(NAME).withDeprecation("sourceRegex", "source-regex");

    public static ParseField FIELD = new ParseField("field");
    public static ParseField REGEX = new ParseField("regex");
    public static ParseField LOAD_FROM_SOURCE = new ParseField("load_from_source");
    public static ParseField NGRAM_FIELD = new ParseField("ngram_field");
    public static ParseField GRAM_SIZE = new ParseField("gram_size");
    private final String field;
    private final String regex;
    private Boolean loadFromSource;
    private String ngramField;
    private Integer gramSize;
    private final Settings settings;

    /**
     * Start building.
     *
     * @param field the field to load and run the regex against
     * @param regex the regex to run
     */
    public SourceRegexQueryBuilder(String field, String regex) {
        this(field, regex, new Settings());
    }

    /**
     * Start building.
     *
     * @param field the field to load and run the regex against
     * @param regex the regex to run
     * @param settings additional settings
     */
    public SourceRegexQueryBuilder(String field, String regex, Settings settings) {
        this.field = Objects.requireNonNull(field);
        this.regex = Objects.requireNonNull(regex);
        this.settings = new Settings();
    }

    public SourceRegexQueryBuilder(StreamInput in) throws IOException {
        field = in.readString();
        regex = in.readString();
        loadFromSource = in.readOptionalBoolean();
        ngramField = in.readOptionalString();
        gramSize = in.readOptionalVInt();
        settings = new Settings(in);
    }

    /**
     * @param loadFromSource should field be loaded from source (true) or from a
     *            stored field (false)?
     * @return this for chaining
     */
    public SourceRegexQueryBuilder loadFromSource(boolean loadFromSource) {
        this.loadFromSource = loadFromSource;
        return this;
    }

    /**
     * @param ngramField field containing ngrams used to prefilter checked
     *            documents. If not set then no ngram acceleration is performed.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder ngramField(String ngramField) {
        this.ngramField = ngramField;
        return this;
    }

    /**
     * @param gramSize size of the gram. Defaults to 3 because everyone loves
     *            trigrams.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder gramSize(int gramSize) {
        this.gramSize = gramSize;
        return this;
    }

    /**
     * @param maxExpand Maximum size of range transitions to expand into
     *            single transitions when turning the automaton from the
     *            regex into an acceleration automaton. Its roughly
     *            analogous to the number of characters in a character class
     *            before it is considered a wildcard for optimization
     *            purposes.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder maxExpand(int maxExpand) {
        settings.maxExpand(maxExpand);
        return this;
    }

    /**
     * @param maxStatesTraced the maximum number of automaton states processed
     *            by the regex parsing algorithm. Higher numbers allow more
     *            complex regexes to be processed. Defaults to 10000 which
     *            allows reasonably complex regexes but still limits the regex
     *            processing time to under a second on modern hardware. 0
     *            effectively disabled regexes more complex than exact sequences
     *            of characters
     * @return this for chaining
     */
    public SourceRegexQueryBuilder maxStatesTraced(int maxStatesTraced) {
        settings.maxStatesTraced(maxStatesTraced);
        return this;
    }

    /**
     * @param maxDeterminizedStates the maximum number of automaton states that
     *            Lucene will create at a time when compiling the regex to a
     *            DFA. Higher numbers allow the regex compilation phase to run
     *            for longer and use more memory needed to compile more complex
     *            regexes.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder maxDeterminizedStates(int maxDeterminizedStates) {
        settings.maxDeterminizedStates(maxDeterminizedStates);
        return this;
    }

    /**
     * @param maxNgramsExtracted the maximum number of ngrams extracted from the
     *            regex. This is pretty much the maximum number of term queries
     *            that are exectued per regex. If any more are required to
     *            accurately limit the regex to some document set they are all
     *            assumed to match all documents that match so far. Its crude,
     *            but it limits the number of term queries while degrading
     *            reasonably well.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder maxNgramsExtracted(int maxNgramsExtracted) {
        settings.maxNgramsExtracted(maxNgramsExtracted);
        return this;
    }

    /**
     * @param maxInspect the maximum number of source documents to run the regex
     *            against per shard. All others after that are assumed not to
     *            match. Defaults to Integer.MAX_VALUE.
     * @return this for chaining
     */
    public SourceRegexQueryBuilder maxInspect(int maxInspect) {
        settings.maxInspect(maxInspect);
        return this;
    }

    public SourceRegexQueryBuilder caseSensitive(boolean caseSensitive) {
        settings.caseSensitive(caseSensitive);
        return this;
    }

    public SourceRegexQueryBuilder locale(Locale locale) {
        settings.locale(locale);
        return this;
    }

    /**
     * @param rejectUnaccelerated should the filter reject regexes it cannot
     *            accelerate?
     * @return this for chaining
     */
    public SourceRegexQueryBuilder rejectUnaccelerated(boolean rejectUnaccelerated) {
        settings.rejectUnaccelerated(rejectUnaccelerated);
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD.getPreferredName(), field);
        builder.field(REGEX.getPreferredName(), regex);

        if (loadFromSource != null) {
            builder.field(LOAD_FROM_SOURCE.getPreferredName(), loadFromSource);
        }
        if (ngramField != null) {
            builder.field(NGRAM_FIELD.getPreferredName(), ngramField);
        }
        if (gramSize != null) {
            builder.field(GRAM_SIZE.getPreferredName(), gramSize);
        }
        settings.innerXContent(builder, params);

        builder.endObject();
    }
    
    

    @Override
    public int doHashCode() {
        return Objects.hash(field, gramSize, loadFromSource, ngramField, regex, settings);
    }

    @Override
    public boolean doEquals(SourceRegexQueryBuilder o) {
        return Objects.equals(field, o.field) &&
                Objects.equals(gramSize, o.gramSize) &&
                Objects.equals(ngramField, o.ngramField) &&
                Objects.equals(loadFromSource, o.loadFromSource) &&                
                Objects.equals(regex, o.regex) &&                
                Objects.equals(settings, o.settings);
    }

    /**
     * Field independent settings for the SourceRegexFilter.
     */
    public static class Settings {
        public static ParseField MAX_EXPAND = new ParseField("max_expand");
        public static ParseField MAX_STATES_TRACED = new ParseField("max_states_traced");
        public static ParseField MAX_DETERMINIZED_STATES = new ParseField("max_determinized_states");
        public static ParseField MAX_NGRAMS_EXTRACTED = new ParseField("max_ngrams_extracted");
        public static ParseField MAX_INSPECT = new ParseField("max_inspect");
        public static ParseField CASE_SENSITIVE = new ParseField("case_sensitive");
        public static ParseField LOCALE = new ParseField("locale");
        public static ParseField REJECT_UNACCELERATED = new ParseField("reject_unaccelerated");
        public static ParseField MAX_NGRAM_CLAUSES = new ParseField("max_ngram_clauses");

        private static final int DEFAULT_MAX_EXPAND = 4;
        private static final int DEFAULT_MAX_STATES_TRACED = 10000;
        private static final int DEFAULT_MAX_DETERMINIZED_STATES = 20000;
        private static final int DEFAULT_MAX_NGRAMS_EXTRACTED = 100;
        private static final int DEFAULT_MAX_INSPECT = Integer.MAX_VALUE;
        private static final boolean DEFAULT_CASE_SENSITIVE = false;
        private static final Locale DEFAULT_LOCALE = Locale.ROOT;
        private static final boolean DEFAULT_REJECT_UNACCELERATED = false;
        private static final int DEFAULT_MAX_BOOLEAN_CLAUSES = ExpressionRewriter.MAX_BOOLEAN_CLAUSES;

        private int maxExpand = DEFAULT_MAX_EXPAND;
        private int maxStatesTraced = DEFAULT_MAX_STATES_TRACED;
        private int maxDeterminizedStates = DEFAULT_MAX_DETERMINIZED_STATES;
        private int maxNgramsExtracted = DEFAULT_MAX_NGRAMS_EXTRACTED;
        /**
         * @deprecated use a generic time limiting collector
         */
        @Deprecated
        private int maxInspect = DEFAULT_MAX_INSPECT;
        private boolean caseSensitive = DEFAULT_CASE_SENSITIVE;
        private Locale locale = DEFAULT_LOCALE;
        private boolean rejectUnaccelerated = DEFAULT_REJECT_UNACCELERATED;
        private int maxNgramClauses = DEFAULT_MAX_BOOLEAN_CLAUSES;

        private Settings() {}

        private Settings(StreamInput in) throws IOException {
            maxExpand = in.readVInt();
            maxStatesTraced = in.readVInt();
            maxDeterminizedStates = in.readVInt();
            maxNgramsExtracted = in.readVInt();
            maxInspect = in.readVInt();
            caseSensitive = in.readBoolean();
            locale = LocaleUtils.parse(in.readString());
            rejectUnaccelerated = in.readBoolean();
            maxNgramClauses = in.readVInt();
        }
        /**
         * @param maxExpand Maximum size of range transitions to expand into
         *            single transitions when turning the automaton from the
         *            regex into an acceleration automaton. Its roughly
         *            analogous to the number of characters in a character class
         *            before it is considered a wildcard for optimization
         *            purposes.
         * @return this for chaining
         */
        public Settings maxExpand(int maxExpand) {
            this.maxExpand = maxExpand;
            return this;
        }

        /**
         * @param maxStatesTraced the maximum number of the regex's automata's
         *            states that will be traced when extracting ngrams for
         *            acceleration. If there are more than this many states then
         *            that portion of the regex isn't used for acceleration.
         * @return this for chaining
         */
        public Settings maxStatesTraced(int maxStatesTraced) {
            this.maxStatesTraced = maxStatesTraced;
            return this;
        }

        /**
         * @param maxDeterminizedStates the maximum number of automaton states
         *            that Lucene will create at a time when compiling the regex
         *            to a DFA. Higher numbers allow the regex compilation phase
         *            to run for longer and use more memory needed to compile
         *            more complex regexes.
         * @return this for chaining
         */
        public Settings maxDeterminizedStates(int maxDeterminizedStates) {
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        /**
         * @param maxNgramsExtracted the maximum number of ngrams extracted from
         *            the regex. This is pretty much the maximum number of term
         *            queries that are exectued per regex. If any more are
         *            required to accurately limit the regex to some document
         *            set they are all assumed to match all documents that match
         *            so far. Its crude, but it limits the number of term
         *            queries while degrading reasonably well.
         * @return this for chaining
         */
        public Settings maxNgramsExtracted(int maxNgramsExtracted) {
            this.maxNgramsExtracted = maxNgramsExtracted;
            return this;
        }

        /**
         * @param maxInspect the maximum number of source documents to run the
         *            regex against per shard. All others after that are assumed
         *            not to match. Defaults to Integer.MAX_VALUE.
         * @return this for chaining
         */
        public Settings maxInspect(int maxInspect) {
            this.maxInspect = maxInspect;
            return this;
        }

        public Settings caseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public Settings locale(Locale locale) {
            this.locale = locale;
            return this;
        }

        public Settings rejectUnaccelerated(boolean rejectUnaccelerated) {
            this.rejectUnaccelerated = rejectUnaccelerated;
            return this;
        }

        public int getMaxExpand() {
            return maxExpand;
        }

        public int getMaxStatesTraced() {
            return maxStatesTraced;
        }

        public int getMaxDeterminizedStates() {
            return maxDeterminizedStates;
        }

        public int getMaxNgramsExtracted() {
            return maxNgramsExtracted;
        }

        public int getMaxInspect() {
            return maxInspect;
        }

        public boolean isCaseSensitive() {
            return caseSensitive;
        }

        public Locale getLocale() {
            return locale;
        }

        public boolean isRejectUnaccelerated() {
            return rejectUnaccelerated;
        }

        public int getMaxNgramClauses() {
            return maxNgramClauses;
        }

        public Settings maxNgramClauses(int maxNgramClauses) {
            this.maxNgramClauses = maxNgramClauses;
            return this;
        }

        public XContentBuilder innerXContent(XContentBuilder builder, Params params) throws IOException {
            if (maxExpand != DEFAULT_MAX_EXPAND) {
                builder.field(MAX_EXPAND.getPreferredName(), maxExpand);
            }
            if (maxStatesTraced != DEFAULT_MAX_STATES_TRACED) {
                builder.field(MAX_STATES_TRACED.getPreferredName(), maxStatesTraced);
            }
            if (maxDeterminizedStates != DEFAULT_MAX_DETERMINIZED_STATES) {
                builder.field(MAX_DETERMINIZED_STATES.getPreferredName(), maxDeterminizedStates);
            }
            if (maxNgramsExtracted != DEFAULT_MAX_NGRAMS_EXTRACTED) {
                builder.field(MAX_NGRAMS_EXTRACTED.getPreferredName(), maxNgramsExtracted);
            }
            if (maxInspect != DEFAULT_MAX_INSPECT) {
                builder.field(MAX_INSPECT.getPreferredName(), maxInspect);
            }
            if (caseSensitive != DEFAULT_CASE_SENSITIVE) {
                builder.field(CASE_SENSITIVE.getPreferredName(), caseSensitive);
            }
            if (locale != DEFAULT_LOCALE) {
                builder.field(LOCALE.getPreferredName(), locale);
            }
            if (rejectUnaccelerated != DEFAULT_REJECT_UNACCELERATED) {
                builder.field(REJECT_UNACCELERATED.getPreferredName(), rejectUnaccelerated);
            }
            if (maxNgramClauses != DEFAULT_MAX_BOOLEAN_CLAUSES) {
                builder.field(MAX_NGRAM_CLAUSES.getPreferredName(), maxNgramClauses);
            }
            return builder;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(maxExpand);
            out.writeOptionalVInt(maxStatesTraced);
            out.writeOptionalVInt(maxDeterminizedStates);
            out.writeOptionalVInt(maxNgramsExtracted);
            out.writeOptionalVInt(maxInspect);
            out.writeOptionalBoolean(caseSensitive);
            out.writeBoolean(locale != null);
            if(locale != null) {
                out.writeString(LocaleUtils.toString(locale));
            }
            out.writeOptionalBoolean(rejectUnaccelerated);
            out.writeOptionalVInt(maxNgramClauses);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(regex);
        out.writeOptionalBoolean(loadFromSource);
        out.writeOptionalString(ngramField);
        out.writeOptionalVInt(gramSize);
        settings.writeTo(out);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new SourceRegexQuery(
                field, ngramField, regex, 
                loadFromSource ? FieldValues.loadFromSource() : FieldValues.loadFromStoredField(),
                settings, gramSize );
    }

    public static Optional<SourceRegexQueryBuilder> fromXContent(QueryParseContext context) throws IOException {
        // Stuff for our filter
        String regex = null;
        String fieldPath = null;
        Boolean loadFromSource = true;
        String ngramFieldPath = null;
        int ngramGramSize = 3;
        Settings settings = new Settings();

        XContentParser parser = context.parser();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, FIELD)) {
                    fieldPath = parser.text();
                } else if(context.getParseFieldMatcher().match(currentFieldName, REGEX)) {
                    regex = parser.text();
                } else if(context.getParseFieldMatcher().match(currentFieldName, LOAD_FROM_SOURCE)) {
                    loadFromSource = parser.booleanValue();
                } else if(context.getParseFieldMatcher().match(currentFieldName, NGRAM_FIELD)) {
                    ngramFieldPath = parser.text();
                } else if(context.getParseFieldMatcher().match(currentFieldName, GRAM_SIZE)) {
                    ngramGramSize = parser.intValue();
                } else if(parseInto(settings, currentFieldName, parser, context)) {
                        continue;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[source-regex] filter does not support [" + currentFieldName
                            + "]");
                }
            }
        }

        if (regex == null || "".equals(regex)) {
            throw new ParsingException(parser.getTokenLocation(), "[source-regex] filter must specify [regex]");
        }
        if (fieldPath == null) {
            throw new ParsingException(parser.getTokenLocation(), "[source-regex] filter must specify [field]");
        }
        SourceRegexQueryBuilder builder = new SourceRegexQueryBuilder(fieldPath, regex, settings);
        builder.ngramField(ngramFieldPath);
        builder.loadFromSource(loadFromSource);
        builder.gramSize(ngramGramSize);
        
        return Optional.of(builder);
    }
    
    private static boolean parseInto(Settings settings, String fieldName, XContentParser parser, QueryParseContext context) throws IOException {
        if (context.getParseFieldMatcher().match(fieldName, Settings.MAX_EXPAND)) {
            settings.maxExpand(parser.intValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.MAX_STATES_TRACED)) {
            settings.maxStatesTraced(parser.intValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.MAX_INSPECT)) {
            settings.maxInspect(parser.intValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.MAX_DETERMINIZED_STATES)) {
            settings.maxDeterminizedStates(parser.intValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.MAX_NGRAMS_EXTRACTED)) {
            settings.maxNgramsExtracted(parser.intValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.CASE_SENSITIVE)) {
            settings.caseSensitive(parser.booleanValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.LOCALE)) {
            settings.locale(LocaleUtils.parse(parser.text()));
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.REJECT_UNACCELERATED)) {
            settings.rejectUnaccelerated(parser.booleanValue());
        } else if(context.getParseFieldMatcher().match(fieldName, Settings.MAX_NGRAM_CLAUSES)) {
            settings.maxNgramClauses(parser.intValue());
        } else {
            return false;
        }
        return true;
    }

}
