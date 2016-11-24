package org.wikimedia.search.extra.regex;


import java.io.IOException;
import java.util.List;

import lombok.EqualsAndHashCode;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.wikimedia.search.extra.regex.SourceRegexQuery.Rechecker;
import org.wikimedia.search.extra.regex.SourceRegexQueryBuilder.Settings;
import org.wikimedia.search.extra.util.FieldValues;
import org.wikimedia.search.extra.util.FieldValues.Loader;

/**
 * Unaccelerated source_regex query.
 * It will scan all the docs in the index.
 */
@EqualsAndHashCode(callSuper=false)
class UnacceleratedSourceRegexQuery extends Query {
    protected final Rechecker rechecker;
    protected final String fieldPath;
    protected final FieldValues.Loader loader;
    protected final Settings settings;

    /**
     * A new accelerated regex query
     * @param rechecker the rechecker used to perform the costly regex on doc content
     * @param fieldPath the path to the field where the doc content is stored
     * @param loader the loader used to load the field content
     * @param settings the regex settings
     */
    public UnacceleratedSourceRegexQuery(Rechecker rechecker, String fieldPath, Loader loader, Settings settings) {
        super();
        this.rechecker = rechecker;
        this.fieldPath = fieldPath;
        this.loader = loader;
        this.settings = settings;
    }

    @Override
    public String toString(String field) {
        return "source_regex(unaccelerated):" + field;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new ConstantScoreWeight(this) {
            // TODO: Get rid of this shared mutable state, we should be able to use
            // the generic timeout system.
            private final MutableValueInt inspected = new MutableValueInt();

            @Override
            public Scorer scorer(final LeafReaderContext context) throws IOException {
                // We can stop matching early if we are allowed to inspect less
                // doc than the number of docs available in this segment.
                // This is because we use a DocIdSetIterator.all.
                int remaining = settings.getMaxInspect() - inspected.value;
                if(remaining < 0) {
                    remaining = 0;
                }
                int maxDoc = remaining > context.reader().maxDoc() ? context.reader().maxDoc() : remaining;
                final DocIdSetIterator approximation = DocIdSetIterator.all(maxDoc);
                return new ConstantScoreScorer(this, 1f, new RegexTwoPhaseIterator(approximation, context, inspected));
            }
        };
    }

    protected class RegexTwoPhaseIterator extends TwoPhaseIterator {
        private final LeafReaderContext context;
        private MutableValueInt inspected;

        protected RegexTwoPhaseIterator(DocIdSetIterator approximation, LeafReaderContext context, MutableValueInt inspected) {
            super(approximation);
            this.context = context;
            this.inspected = inspected;
        }

        @Override
        public boolean matches() throws IOException {
            if (inspected.value >= settings.getMaxInspect()) {
                return false;
            }
            List<String> values = loader.load(fieldPath, context.reader(), approximation.docID());
            inspected.value++;
            return rechecker.recheck(values);
        }

        @Override
        public float matchCost() {
            /*
             * the recheck phase is costly and depends mostly on doc size. We
             * set a very large base cost to reflect the fact that we will load
             * the field data (I/O and mem) then we add a rechecker specific
             * cost that depends on the number of states.
             */
            return 10000f + rechecker.getCost();
        }
    }

}
