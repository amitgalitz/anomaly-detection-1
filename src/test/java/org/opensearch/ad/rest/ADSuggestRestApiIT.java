/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI;
import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;

import java.util.Locale;
import java.util.Map;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Config;

import com.google.common.collect.ImmutableMap;

public class ADSuggestRestApiIT extends AbstractADSyntheticDataTest {

    protected static final String SUGGEST_INTERVAL_URI;
    protected static final String SUGGEST_INTERVAL_HISTORY_URI;
    protected static final String SUGGEST_INTERVAL_HISTORY_DELAY_URI;

    static {
        SUGGEST_INTERVAL_URI = String
            .format(Locale.ROOT, "%s/%s/%s", AD_BASE_DETECTORS_URI, SUGGEST, AnomalyDetector.DETECTION_INTERVAL_FIELD);
        SUGGEST_INTERVAL_HISTORY_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s,%s",
                AD_BASE_DETECTORS_URI,
                SUGGEST,
                AnomalyDetector.DETECTION_INTERVAL_FIELD,
                Config.HISTORY_INTERVAL_FIELD
            );
        SUGGEST_INTERVAL_HISTORY_DELAY_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s,%s,%s",
                AD_BASE_DETECTORS_URI,
                SUGGEST,
                AnomalyDetector.DETECTION_INTERVAL_FIELD,
                Config.HISTORY_INTERVAL_FIELD,
                AnomalyDetector.WINDOW_DELAY_FIELD
            );
    }

    public void testSuggestInterval() throws Exception {
        // Load synthetic data for 1-minute interval
        loadSyntheticData(200);

        String detectorDef = "{\n"
            + "    \"name\": \"test-detector\",\n"
            + "    \"description\": \"Test detector\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\""
            + SYNTHETIC_DATASET_NAME
            + "\"],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_name\": \"feature1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"feature1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detectorDef),
                null
            );

        assertEquals("Suggest detector interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));
    }

    public void testSuggestMultipleParams() throws Exception {
        // Load synthetic data for 1-minute interval
        loadSyntheticData(200);

        String detectorDef = "{\n"
            + "    \"name\": \"test-detector\",\n"
            + "    \"description\": \"Test detector\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\""
            + SYNTHETIC_DATASET_NAME
            + "\"],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_name\": \"feature1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"feature1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_HISTORY_DELAY_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detectorDef),
                null
            );

        assertEquals("Suggest detector params failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);

        // Check interval suggestion
        Map<String, Object> intervalSuggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) intervalSuggestions.get("interval"));
        assertEquals("Minutes", intervalSuggestions.get("unit"));

        // Check history suggestion
        int historySuggestions = ((Integer) responseMap.get("history"));
        assertTrue("History should be positive", historySuggestions > 0);

        // Check window delay suggestion
        Map<String, Object> windowDelaySuggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("windowDelay"))
            .get("period");
        assertTrue("Window delay should be non-negative", (int) windowDelaySuggestions.get("interval") >= 0);
        assertEquals("Minutes", windowDelaySuggestions.get("unit"));
    }
}
