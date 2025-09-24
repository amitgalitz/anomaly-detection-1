/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.BaseSuggestConfigParamTransportAction;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class SuggestAnomalyDetectorParamTransportAction extends BaseSuggestConfigParamTransportAction {
    public static final Logger logger = LogManager.getLogger(SuggestAnomalyDetectorParamTransportAction.class);

    @Inject
    public SuggestAnomalyDetectorParamTransportAction(
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        Settings settings,
        ADIndexManagement anomalyDetectionIndices,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(
            SuggestAnomalyDetectorParamAction.NAME,
            client,
            clientUtil,
            clusterService,
            settings,
            actionFilters,
            transportService,
            AD_FILTER_BY_BACKEND_ROLES,
            AnalysisType.AD,
            searchFeatureDao,
            namedWriteableRegistry
        );
    }

    @Override
    public void suggestExecute(
        SuggestConfigParamRequest request,
        User user,
        ThreadContext.StoredContext storedContext,
        ActionListener<SuggestConfigParamResponse> listener
    ) {
        storedContext.restore();
        // Get parameters to suggest - no need to filter HORIZON since AD SuggestName doesn't have it
        Set<SuggestName> params = getParametersToSuggestAD(request.getParam());

        if (params.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(CommonMessages.NOT_EXISTENT_SUGGEST_TYPE);
            listener.onFailure(validationException);
            return;
        }

        Config config = request.getConfig();

        int responseSize = params.size();
        // history suggest interval too as history suggest depends on interval suggest
        if (params.contains(SuggestName.HISTORY) && params.contains(SuggestName.INTERVAL)) {
            responseSize -= 1;
        }

        MultiResponsesDelegateActionListener<SuggestConfigParamResponse> delegateListener =
            new MultiResponsesDelegateActionListener<SuggestConfigParamResponse>(
                listener,
                responseSize,
                CommonMessages.FAIL_SUGGEST_ERR_MSG,
                false
            );

        // history suggest interval too as history suggest depends on interval suggest
        if (params.contains(SuggestName.HISTORY)) {
            suggestHistory(request.getConfig(), user, request.getRequestTimeout(), params.contains(SuggestName.INTERVAL), delegateListener);
        } else if (params.contains(SuggestName.INTERVAL)) {
            suggestInterval(
                request.getConfig(),
                user,
                request.getRequestTimeout(),
                ActionListener
                    .wrap(
                        intervalEntity -> delegateListener
                            .onResponse(new SuggestConfigParamResponse.Builder().interval(intervalEntity.getLeft()).build()),
                        delegateListener::onFailure
                    )
            );
        }

        if (params.contains(SuggestName.WINDOW_DELAY)) {
            suggestWindowDelay(request.getConfig(), user, request.getRequestTimeout(), delegateListener);
        }
    }

    private Set<SuggestName> getParametersToSuggestAD(String typesStr) {
        return java.util.Arrays.stream(typesStr.split(",")).map(String::trim).filter(type -> !type.isEmpty()).map(type -> {
            try {
                return SuggestName.getName(type);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }).filter(java.util.Objects::nonNull).collect(Collectors.toSet());
    }
}
