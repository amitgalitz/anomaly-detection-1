/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;

public class SuggestConfigParamRequest extends ActionRequest implements DocRequest {

    private final AnalysisType context;
    private final Config config;
    private final String param;
    private final TimeValue requestTimeout;

    public SuggestConfigParamRequest(StreamInput in) throws IOException {
        super(in);
        context = in.readEnum(AnalysisType.class);
        if (getContext().isAD()) {
            config = new AnomalyDetector(in);
        } else if (getContext().isForecast()) {
            config = new Forecaster(in);
        } else {
            throw new UnsupportedOperationException("This method is not supported");
        }

        param = in.readString();
        requestTimeout = in.readTimeValue();
    }

    public SuggestConfigParamRequest(AnalysisType context, Config config, String param, TimeValue requestTimeout) {
        this.context = context;
        this.config = config;
        this.param = param;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(getContext());
        config.writeTo(out);
        out.writeString(param);
        out.writeTimeValue(requestTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public Config getConfig() {
        return config;
    }

    public String getParam() {
        return param;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    public AnalysisType getContext() {
        return context;
    }

    @Override
    public String index() {
        if (context.isAD()) {
            return ADIndex.CONFIG.getIndexName();
        }
        return ForecastIndex.CONFIG.getIndexName();
    }

    @Override
    public String id() {
        return config.getId();
    }

    public static SuggestConfigParamRequest fromActionRequest(
        final ActionRequest actionRequest,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        if (actionRequest instanceof SuggestConfigParamRequest) {
            return (SuggestConfigParamRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (
                StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
                NamedWriteableAwareStreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                return new SuggestConfigParamRequest(namedInput);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into SuggestConfigParamRequest", e);
        }
    }
}
