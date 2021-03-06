package org.wikimedia.search.extra.safer;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.wikimedia.search.extra.safer.Safeifier.Action;

public class DefaultQueryExplodingSafeifierActions {
    public static void register(Safeifier safeifier) {
        safeifier.register(BooleanQuery.class, BOOLEAN_QUERY_ACTION);
        safeifier.register(DisjunctionMaxQuery.class, DISJUNCTION_MAX_QUERY_ACTION);
        safeifier.register(FilteredQuery.class, FILTERED_QUERY_ACTION);
        safeifier.register(XFilteredQuery.class, XFILTERED_QUERY_ACTION);
        safeifier.register(ConstantScoreQuery.class, CONSTANT_SCORE_QUERY_ACTION);
    }

    private static final Action<BooleanQuery, BooleanQuery> BOOLEAN_QUERY_ACTION = new Action<BooleanQuery, BooleanQuery>() {
        @Override
        public BooleanQuery apply(Safeifier safeifier, BooleanQuery bq) {
            BooleanQuery replaced = new BooleanQuery();
            replaced.setBoost(bq.getBoost());
            for (BooleanClause clause : bq.getClauses()) {
                replaced.add(safeifier.safeify(clause.getQuery()), clause.getOccur());
            }
            return replaced;
        }
    };
    private static final Action<DisjunctionMaxQuery, DisjunctionMaxQuery> DISJUNCTION_MAX_QUERY_ACTION = new Action<DisjunctionMaxQuery, DisjunctionMaxQuery>() {
        @Override
        public DisjunctionMaxQuery apply(Safeifier safeifier, DisjunctionMaxQuery dmq) {
            DisjunctionMaxQuery replaced = new DisjunctionMaxQuery(dmq.getTieBreakerMultiplier());
            replaced.setBoost(dmq.getBoost());
            for (Query disjunct : dmq.getDisjuncts()) {
                replaced.add(safeifier.safeify(disjunct));
            }
            return replaced;
        }
    };
    private static final Action<FilteredQuery, XFilteredQuery> FILTERED_QUERY_ACTION = new Action<FilteredQuery, XFilteredQuery>() {
        @Override
        public XFilteredQuery apply(Safeifier safeifier, FilteredQuery fq) {
            // FilterQuery is unsafe and banned from Elasticsearch so we
            // just convert....
            XFilteredQuery newQuery = new XFilteredQuery(safeifier.safeify(fq.getQuery()), fq.getFilter(), fq.getFilterStrategy());
            // TODO safeify filters
            newQuery.setBoost(fq.getBoost());
            return newQuery;
        }
    };
    private static final Action<XFilteredQuery, XFilteredQuery> XFILTERED_QUERY_ACTION = new Action<XFilteredQuery, XFilteredQuery>() {
        @Override
        public XFilteredQuery apply(Safeifier safeifier, XFilteredQuery fq) {
            XFilteredQuery newQuery = new XFilteredQuery(safeifier.safeify(fq.getQuery()), fq.getFilter(),
            /*
             * WOW! I can't actually read the old filter strategy so we just
             * have guess.... fq.getFilterStrategy() doesn't exist
             */
            XFilteredQuery.CUSTOM_FILTER_STRATEGY);
            // TODO safeify filters
            newQuery.setBoost(fq.getBoost());
            return newQuery;
        }
    };
    private static final Action<ConstantScoreQuery, ConstantScoreQuery> CONSTANT_SCORE_QUERY_ACTION = new Action<ConstantScoreQuery, ConstantScoreQuery>() {
        @Override
        public ConstantScoreQuery apply(Safeifier safeifier, ConstantScoreQuery csq) {
            // TODO safeify filters
            if (csq.getQuery() != null) {
                ConstantScoreQuery newQuery = new ConstantScoreQuery(safeifier.safeify(csq.getQuery()));
                newQuery.setBoost(csq.getBoost());
                return newQuery;
            }
            return csq;
        }
    };
}
