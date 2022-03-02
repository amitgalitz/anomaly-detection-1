/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CONFIG_BUCKET_MINIMUM_SUCCESS_RATE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_TIMES_DECREASING_INTERVAL;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.TOP_VALIDATE_TIMEOUT_IN_MILLIS;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.model.TimeConfiguration;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * <p>This class executes all validation checks that are not blocking on the 'model' level.
 * This mostly involves checking if the data is generally dense enough to complete model training
 * which is based on if enough buckets in the last x intervals have at least 1 document present.</p>
 * <p>Initially different bucket aggregations are executed with with every configuration applied and with
 * different varying intervals in order to find the best interval for the data. If no interval is found with all
 * configuration applied then each configuration is tested sequentially for sparsity</p>
 */
// TODO: potentially change where this is located
public class ModelValidationActionHandler {
    protected static final String AGG_NAME_TOP = "top_agg";
    protected static final String AGGREGATION = "agg";
    protected final AnomalyDetector anomalyDetector;
    protected final ClusterService clusterService;
    protected final Logger logger = LogManager.getLogger(AbstractAnomalyDetectorActionHandler.class);
    protected final TimeValue requestTimeout;
    protected final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();
    protected final Client client;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ActionListener<ValidateAnomalyDetectorResponse> listener;
    protected final SearchFeatureDao searchFeatureDao;
    protected final Clock clock;
    protected final String validationType;

    /**
     * Constructor function.
     *
     * @param clusterService                  ClusterService
     * @param client                          ES node client that executes actions on the local node
     * @param listener                        ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetector                 anomaly detector instance
     * @param requestTimeout                  request time out configuration
     * @param xContentRegistry                Registry which is used for XContentParser
     * @param searchFeatureDao                Search feature DAO
     * @param validationType                  Specified type for validation
     * @param clock                           clock object to know when to timeout
     */
    public ModelValidationActionHandler(
        ClusterService clusterService,
        Client client,
        ActionListener<ValidateAnomalyDetectorResponse> listener,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.listener = listener;
        this.anomalyDetector = anomalyDetector;
        this.requestTimeout = requestTimeout;
        this.xContentRegistry = xContentRegistry;
        this.searchFeatureDao = searchFeatureDao;
        this.validationType = validationType;
        this.clock = clock;
    }

    // Need to first check if multi entity detector or not before doing any sort of validation.
    // If detector is HCAD then we will find the top entity and treat as single entity for
    // validation purposes
    public void checkIfMultiEntityDetector() {
        ActionListener<Map<String, Object>> recommendationListener = ActionListener
            .wrap(topEntity -> getLatestDateForValidation(topEntity), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get top entity for categorical field", exception);
            });
        if (anomalyDetector.isMultientityDetector()) {
            getTopEntity(recommendationListener);
        } else {
            recommendationListener.onResponse(Collections.emptyMap());
        }
    }

    // For single category HCAD, this method uses bucket aggregation and sort to get the category field
    // that have the highest document count in order to use that top entity for further validation
    // For multi-category HCADs we use a composite aggregation to find the top fields for the entity
    // with the highest doc count.
    private void getTopEntity(ActionListener<Map<String, Object>> topEntityListener) {
        AggregationBuilder bucketAggs;
        Map<String, Object> topKeys = new HashMap<>();
        if (anomalyDetector.getCategoryField().size() == 1) {
            bucketAggs = AggregationBuilders
                .terms(AGG_NAME_TOP)
                .field(anomalyDetector.getCategoryField().get(0))
                .order(BucketOrder.count(true));
        } else {
            bucketAggs = AggregationBuilders
                .composite(
                    AGG_NAME_TOP,
                    anomalyDetector
                        .getCategoryField()
                        .stream()
                        .map(f -> new TermsValuesSourceBuilder(f).field(f))
                        .collect(Collectors.toList())
                )
                .size(1000)
                .subAggregation(
                    PipelineAggregatorBuilders
                        .bucketSort("bucketSort", Collections.singletonList(new FieldSortBuilder("_count").order(SortOrder.DESC)))
                        .size(1)
                );
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(bucketAggs).trackTotalHits(false).size(0);
        SearchRequest searchRequest = new SearchRequest()
            .indices(anomalyDetector.getIndices().toArray(new String[0]))
            .source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                topEntityListener.onResponse(Collections.emptyMap());
                return;
            }
            if (anomalyDetector.getCategoryField().size() == 1) {
                Terms entities = aggs.get(AGG_NAME_TOP);
                Object key = entities
                    .getBuckets()
                    .stream()
                    .max(Comparator.comparingInt(entry -> (int) entry.getDocCount()))
                    .map(MultiBucketsAggregation.Bucket::getKeyAsString)
                    .orElse(null);
                topKeys.put(anomalyDetector.getCategoryField().get(0), key);
            } else {
                CompositeAggregation compositeAgg = aggs.get(AGG_NAME_TOP);
                topKeys
                    .putAll(
                        compositeAgg
                            .getBuckets()
                            .stream()
                            .flatMap(bucket -> bucket.getKey().entrySet().stream()) // this would create a flattened stream of map entries
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))
                    );
            }
            for (Map.Entry<String, Object> entry : topKeys.entrySet()) {
                if (entry.getValue() == null) {
                    topEntityListener.onResponse(Collections.emptyMap());
                    return;
                }
            }
            topEntityListener.onResponse(topKeys);
        }, topEntityListener::onFailure));
    }

    private void getLatestDateForValidation(Map<String, Object> topEntity) {
        ActionListener<Optional<Long>> latestTimeListener = ActionListener
            .wrap(latest -> getSampleRangesForValidationChecks(latest, anomalyDetector, listener, topEntity), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to create search request for last data point", exception);
            });
        searchFeatureDao.getLatestDataTime(anomalyDetector, latestTimeListener);
    }

    private void getSampleRangesForValidationChecks(
        Optional<Long> latestTime,
        AnomalyDetector detector,
        ActionListener<ValidateAnomalyDetectorResponse> listener,
        Map<String, Object> topEntity
    ) {
        if (!latestTime.isPresent() || latestTime.get() <= 0) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.TIME_FIELD_NOT_ENOUGH_HISTORICAL_DATA,
                        DetectorValidationIssueType.TIMEFIELD_FIELD,
                        ValidationAspect.MODEL
                    )
                );
            return;
        }
        long timeRangeEnd = Math.min(Instant.now().toEpochMilli(), latestTime.get());
        try {
            getBucketAggregates(timeRangeEnd, listener, topEntity);
        } catch (IOException e) {
            listener.onFailure(new EndRunException(detector.getDetectorId(), CommonErrorMessages.INVALID_SEARCH_QUERY_MSG, e, true));
        }
    }

    private void getBucketAggregates(
        long latestTime,
        ActionListener<ValidateAnomalyDetectorResponse> listener,
        Map<String, Object> topEntity
    ) throws IOException {
        List<String> featureFields = ParseUtils.getFeatureFieldNames(anomalyDetector, xContentRegistry);
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(anomalyDetector.getFilterQuery());
        if (anomalyDetector.isMultientityDetector()) {
            if (topEntity.isEmpty()) {
                listener
                    .onFailure(
                        new ADValidationException(
                            CommonErrorMessages.CATEGORY_FIELD_NO_DATA,
                            DetectorValidationIssueType.CATEGORY,
                            ValidationAspect.MODEL
                        )
                    );
                return;
            }
            for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
                query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
        }

        for (String featureField : featureFields) {
            query.filter(QueryBuilders.existsQuery(featureField));
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(query)
            .aggregation(aggregation)
            .size(0)
            .timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        ActionListener<IntervalTimeConfiguration> intervalListener = ActionListener
            .wrap(interval -> processIntervalRecommendation(interval, latestTime), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get interval recommendation", exception);
            });
        client
            .search(
                searchRequest,
                new ModelValidationActionHandler.DetectorIntervalRecommendationListener(
                    intervalListener,
                    searchRequest.source(),
                    (IntervalTimeConfiguration) anomalyDetector.getDetectionInterval(),
                    clock.millis() + TOP_VALIDATE_TIMEOUT_IN_MILLIS,
                    latestTime,
                    false,
                    MAX_TIMES_DECREASING_INTERVAL
                )
            );
    }

    private double processBucketAggregationResults(Histogram buckets) {
        int docCountOverOne = 0;
        // For each entry
        for (Histogram.Bucket entry : buckets.getBuckets()) {
            if (entry.getDocCount() > 0) {
                docCountOverOne++;
            }
        }
        return (docCountOverOne / (double) getNumberOfSamples());
    }

    /**
     * ActionListener class to handle execution of multiple bucket aggregations one after the other
     * Bucket aggregation with different interval lengths are executed one by one to check if the data is dense enough
     * We only need to execute the next query if the previous one led to data that is too sparse.
     */
    class DetectorIntervalRecommendationListener implements ActionListener<SearchResponse> {
        private final ActionListener<IntervalTimeConfiguration> intervalListener;
        SearchSourceBuilder searchSourceBuilder;
        IntervalTimeConfiguration detectorInterval;
        private final long expirationEpochMs;
        private final long latestTime;
        boolean decreasingInterval;
        int numTimesDecreasing; // maximum amount of times we will try decreasing interval for recommendation

        DetectorIntervalRecommendationListener(
            ActionListener<IntervalTimeConfiguration> intervalListener,
            SearchSourceBuilder searchSourceBuilder,
            IntervalTimeConfiguration detectorInterval,
            long expirationEpochMs,
            long latestTime,
            boolean decreasingInterval,
            int numTimesDecreasing
        ) {
            this.intervalListener = intervalListener;
            this.searchSourceBuilder = searchSourceBuilder;
            this.detectorInterval = detectorInterval;
            this.expirationEpochMs = expirationEpochMs;
            this.latestTime = latestTime;
            this.decreasingInterval = decreasingInterval;
            this.numTimesDecreasing = numTimesDecreasing;
        }

        private AggregationBuilder getNewAggregationBuilder(long newInterval) {
            return AggregationBuilders
                .dateHistogram(AGGREGATION)
                .field(anomalyDetector.getTimeField())
                .minDocCount(0)
                .hardBounds(getTimeRangeBounds(latestTime, new IntervalTimeConfiguration(newInterval, ChronoUnit.MINUTES)))
                .fixedInterval(DateHistogramInterval.minutes((int) newInterval));
        }

        private long convertIntervalToMinutes(IntervalTimeConfiguration interval) {
            long currentInterval = interval.getInterval();
            if (interval.getUnit() == ChronoUnit.MILLIS) {
                currentInterval /= 60000;
            }
            return currentInterval;
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                Histogram aggregate = checkBucketResultErrors(response);
                if (aggregate == null) {
                    return;
                }

                long newInterval;
                if (decreasingInterval) {
                    newInterval = (long) Math
                        .ceil(convertIntervalToMinutes(detectorInterval) * INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER);
                } else {
                    newInterval = (long) Math
                        .ceil(convertIntervalToMinutes(detectorInterval) * INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER);
                }
                double fullBucketRate = processBucketAggregationResults(aggregate);
                // If rate is above success minimum then return interval suggestion.
                if (fullBucketRate > INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE) {
                    intervalListener.onResponse(this.detectorInterval);
                } else if (expirationEpochMs < clock.millis()) {
                    listener
                        .onFailure(
                            new ADValidationException(
                                CommonErrorMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                                DetectorValidationIssueType.MODEL_VALIDATION_ISSUE,
                                ValidationAspect.MODEL
                            )
                        );
                    logger.info("Timed out getting interval recommendation");
                    // keep trying higher intervals as new interval is below max, and we aren't decreasing yet
                } else if (newInterval < MAX_INTERVAL_REC_LENGTH_IN_MINUTES && !decreasingInterval) {
                    searchWithDifferentInterval(newInterval);
                    // The below block is executed only the first time when new interval is above max and
                    // we aren't decreasing yet, at this point
                } else if (newInterval >= MAX_INTERVAL_REC_LENGTH_IN_MINUTES && !decreasingInterval) {
                    IntervalTimeConfiguration givenInterval = (IntervalTimeConfiguration) anomalyDetector.getDetectionInterval();
                    this.detectorInterval = new IntervalTimeConfiguration(
                        (long) Math.ceil(convertIntervalToMinutes(givenInterval) * INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER),
                        ChronoUnit.MINUTES
                    );
                    this.decreasingInterval = true;
                    this.numTimesDecreasing -= 1;
                    // Searching again using an updated interval
                    SearchSourceBuilder updatedSearchSourceBuilder = getSearchSourceBuilder(
                        searchSourceBuilder.query(),
                        getNewAggregationBuilder(newInterval)
                    );
                    client
                        .search(
                            new SearchRequest()
                                .indices(anomalyDetector.getIndices().toArray(new String[0]))
                                .source(updatedSearchSourceBuilder),
                            this
                        );
                    // In this case decreasingInterval has to be true already, so we will stop
                    // when the next new interval is below or equal to 0, or we have decreased up to max times
                } else if (numTimesDecreasing >= 0 || newInterval <= 0) {
                    this.numTimesDecreasing -= 1;
                    searchWithDifferentInterval(newInterval);
                    // this case means all intervals up to max interval recommendation length and down to either
                    // 0 or until we tried 10 lower intervals than the one given have been tried
                    // which further means the next step is to go through A/B validation checks
                } else {
                    intervalListener.onResponse(null);
                }

            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void searchWithDifferentInterval(long newIntervalValue) {
            this.detectorInterval = new IntervalTimeConfiguration(newIntervalValue, ChronoUnit.MINUTES);
            // Searching again using an updated interval
            SearchSourceBuilder updatedSearchSourceBuilder = getSearchSourceBuilder(
                searchSourceBuilder.query(),
                getNewAggregationBuilder(newIntervalValue)
            );
            client
                .search(
                    new SearchRequest().indices(anomalyDetector.getIndices().toArray(new String[0])).source(updatedSearchSourceBuilder),
                    this
                );
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to recommend new interval", e);
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        DetectorValidationIssueType.MODEL_VALIDATION_ISSUE,
                        ValidationAspect.MODEL
                    )
                );
        }
    }

    private void processIntervalRecommendation(IntervalTimeConfiguration interval, long latestTime) {
        // if interval suggestion is null that means no interval could be found with all the configurations
        // applied, our next step then is to check density just with the raw data and then add each configuration
        // one at a time to try and find root cause of low density
        if (interval == null) {
            checkRawDataSparsity(latestTime);
        } else {
            if (interval.equals(anomalyDetector.getDetectionInterval())) {
                logger.info("Using the current interval there is enough dense data ");
                // The rate of buckets with at least 1 doc with given interval is above the success rate
                listener.onResponse(null);
                return;
            }
            // return response with interval recommendation
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.DETECTOR_INTERVAL_REC + interval.getInterval(),
                        DetectorValidationIssueType.DETECTION_INTERVAL,
                        ValidationAspect.MODEL,
                        interval
                    )
                );
        }
    }

    private AggregationBuilder getBucketAggregation(long latestTime) {
        return AggregationBuilders
            .dateHistogram(AGGREGATION)
            .field(anomalyDetector.getTimeField())
            .minDocCount(0)
            .hardBounds(getTimeRangeBounds(latestTime, (IntervalTimeConfiguration) anomalyDetector.getDetectionInterval()))
            .fixedInterval(DateHistogramInterval.minutes((int) anomalyDetector.getDetectorIntervalInMinutes()));
    }

    private SearchSourceBuilder getSearchSourceBuilder(QueryBuilder query, AggregationBuilder aggregation) {
        return new SearchSourceBuilder().query(query).aggregation(aggregation).size(0).timeout(requestTimeout);
    }

    private void checkRawDataSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(aggregation).size(0).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> processRawDataResults(response, latestTime), listener::onFailure));
    }

    private Histogram checkBucketResultErrors(SearchResponse response) {
        Aggregations aggs = response.getAggregations();
        if (aggs == null) {
            // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
            // the large amounts of changes there). For this reason I'm not throwing a SearchException but instead a validation exception
            // which will be converted to validation response.
            logger.warn("Unexpected null aggregation.");
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        DetectorValidationIssueType.MODEL_VALIDATION_ISSUE,
                        ValidationAspect.MODEL
                    )
                );
            return null;
        }
        Histogram aggregate = aggs.get(AGGREGATION);
        if (aggregate == null) {
            listener.onFailure(new IllegalArgumentException("Failed to find valid aggregation result"));
            return null;
        }
        return aggregate;
    }

    private void processRawDataResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.RAW_DATA_TOO_SPARSE,
                        DetectorValidationIssueType.INDICES,
                        ValidationAspect.MODEL
                    )
                );
        } else {
            checkDataFilterSparsity(latestTime);
        }
    }

    private void checkDataFilterSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(anomalyDetector.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> processDataFilterResults(response, latestTime), listener::onFailure));
    }

    private void processDataFilterResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.FILTER_QUERY_TOO_SPARSE,
                        DetectorValidationIssueType.FILTER_QUERY,
                        ValidationAspect.MODEL
                    )
                );
            // blocks below are executed if data is dense enough with filter query applied.
            // If HCAD then category fields will be added to bucket aggregation to see if they
            // are the root cause of the issues and if not the feature queries will be checked for sparsity
        } else if (anomalyDetector.isMultientityDetector()) {
            getTopEntityForCategoryField(latestTime);
        } else {
            try {
                checkFeatureQuery(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void getTopEntityForCategoryField(long latestTime) {
        ActionListener<Map<String, Object>> getTopEntityListener = ActionListener
            .wrap(topEntity -> checkCategoryFieldSparsity(topEntity, latestTime), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get top entity for categorical field", exception);
                return;
            });
        getTopEntity(getTopEntityListener);
    }

    private void checkCategoryFieldSparsity(Map<String, Object> topEntity, long latestTime) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(anomalyDetector.getFilterQuery());
        for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
            query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
        }
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> processTopEntityResults(response, latestTime), listener::onFailure));
    }

    private void processTopEntityResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.CATEGORY_FIELD_TOO_SPARSE,
                        DetectorValidationIssueType.CATEGORY,
                        ValidationAspect.MODEL
                    )
                );
        } else {
            try {
                checkFeatureQuery(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void processFeatureQuery(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }

        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.FEATURE_QUERY_TOO_SPARSE,
                        DetectorValidationIssueType.FEATURE_ATTRIBUTES,
                        ValidationAspect.MODEL
                    )
                );
            return;
        }
        windowDelayRecommendation(latestTime);
    }

    private void checkFeatureQuery(long latestTime) throws IOException {
        List<String> featureFields = ParseUtils.getFeatureFieldNames(anomalyDetector, xContentRegistry);
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(anomalyDetector.getFilterQuery());
        for (String featureField : featureFields) {
            query.filter(QueryBuilders.existsQuery(featureField));
        }
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> processFeatureQuery(response, latestTime), listener::onFailure));
    }

    private void windowDelayRecommendation(long latestTime) {
        long delayMillis = timeConfigToMilliSec(anomalyDetector.getWindowDelay());
        if ((Instant.now().toEpochMilli() - latestTime > delayMillis)) {
            long minutesSinceLastStamp = TimeUnit.MILLISECONDS.toMinutes(Instant.now().toEpochMilli() - latestTime);
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.WINDOW_DELAY_REC + minutesSinceLastStamp,
                        DetectorValidationIssueType.WINDOW_DELAY,
                        ValidationAspect.MODEL,
                        new IntervalTimeConfiguration(minutesSinceLastStamp, ChronoUnit.MINUTES)
                    )
                );
            return;
        }
        listener.onResponse(null);
    }

    private LongBounds getTimeRangeBounds(long endMillis, IntervalTimeConfiguration detectorIntervalInMinutes) {
        Long detectorInterval = timeConfigToMilliSec(detectorIntervalInMinutes);
        Long startMillis = endMillis - ((long) getNumberOfSamples() * detectorInterval);
        return new LongBounds(startMillis, endMillis);
    }

    private int getNumberOfSamples() {
        long interval = anomalyDetector.getDetectorIntervalInMilliseconds();
        return Math
            .max(
                (int) (Duration.ofHours(AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS).toMillis() / interval),
                AnomalyDetectorSettings.MIN_TRAIN_SAMPLES
            );
    }

    private Long timeConfigToMilliSec(TimeConfiguration config) {
        return Optional.ofNullable((IntervalTimeConfiguration) config).map(t -> t.toDuration().toMillis()).orElse(0L);
    }
}
