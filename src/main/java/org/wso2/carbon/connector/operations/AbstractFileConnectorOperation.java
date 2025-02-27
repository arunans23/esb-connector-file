package org.wso2.carbon.connector.operations;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPBody;
import org.apache.axis2.AxisFault;
import org.apache.commons.lang.StringUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.data.connector.ConnectorResponse;
import org.apache.synapse.data.connector.DefaultConnectorResponse;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.pojo.FileOperationResult;
import org.wso2.carbon.connector.utils.Const;
import org.wso2.carbon.connector.utils.Error;

import java.util.Map;
import java.util.Objects;

public abstract class AbstractFileConnectorOperation extends AbstractConnector {

    abstract public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws ConnectException;

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        String responseVariable = getMediatorParameter(
                messageContext, Const.RESPONSE_VARIABLE, String.class, false
                                                      );
        Boolean overwriteBody = getMediatorParameter(
                messageContext, Const.OVERWRITE_BODY, Boolean.class, false);
        execute(messageContext, responseVariable, overwriteBody);
    }

    protected <T> T getMediatorParameter(
            MessageContext messageContext, String parameterName, Class<T> type, boolean isOptional) {

        Object parameter = getParameter(messageContext, parameterName);
        if (!isOptional && (parameter == null || parameter.toString().isEmpty())) {
            handleException(String.format("Parameter %s is not provided", parameterName), messageContext);
        } else if (parameter == null || parameter.toString().isEmpty()) {
            return null;
        }

        try {
            return parse(Objects.requireNonNull(parameter).toString(), type);
        } catch (IllegalArgumentException e) {
            handleException(String.format(
                    "Parameter %s is not of type %s", parameterName, type.getName()
                                         ), messageContext);
        }

        return null;
    }

    protected <T> T getProperty(
            MessageContext messageContext, String propertyName, Class<T> type, boolean isOptional) {

        Object property = messageContext.getProperty(propertyName);
        if (!isOptional && (property == null || property.toString().isEmpty())) {
            handleException(String.format("Property %s is not set", propertyName), messageContext);
        } else if (property == null || property.toString().isEmpty()) {
            return null;
        }

        try {
            return parse(Objects.requireNonNull(property).toString(), type);
        } catch (IllegalArgumentException e) {
            handleException(String.format(
                    "Property %s is not of type %s", propertyName, type.getName()
                                         ), messageContext);
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T parse(String value, Class<T> type) throws IllegalArgumentException {
        if (type == Integer.class) {
            return (T) Integer.valueOf(value);
        } else if (type == Double.class) {
            return (T) Double.valueOf(value);
        } else if (type == Boolean.class) {
            return (T) Boolean.valueOf(value);
        } else if (type == String.class) {
            return (T) value;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public JsonObject generateOperationResult(MessageContext msgContext, FileOperationResult result) {
        JsonObject jsonResult = new JsonObject();

        jsonResult.addProperty("operation", result.getOperation());
        jsonResult.addProperty("success", result.isSuccessful());

        if (result.getWrittenBytes() != 0) {
            jsonResult.addProperty("writtenBytes", result.getWrittenBytes());
        }

        if (result.getError() != null) {
            setErrorPropertiesToMessage(msgContext, result.getError());

            JsonObject errorJson = new JsonObject();
            errorJson.addProperty("code", result.getError().getErrorCode());
            errorJson.addProperty("message", result.getError().getErrorDetail());
            jsonResult.add("error", errorJson);

            if (StringUtils.isNotEmpty(result.getErrorMessage())) {
                jsonResult.addProperty("errorDetail", result.getErrorMessage());
            }
        }

        return jsonResult;
    }

    public static void setErrorPropertiesToMessage(MessageContext messageContext, Error error) {

        messageContext.setProperty(Const.PROPERTY_ERROR_CODE, error.getErrorCode());
        messageContext.setProperty(Const.PROPERTY_ERROR_MESSAGE, error.getErrorDetail());
        Axis2MessageContext axis2smc = (Axis2MessageContext) messageContext;
        org.apache.axis2.context.MessageContext axis2MessageCtx = axis2smc.getAxis2MessageContext();
        axis2MessageCtx.setProperty(Const.STATUS_CODE, Const.HTTP_STATUS_500);
    }

    protected void handleConnectorResponse(MessageContext messageContext, String responseVariable,
                                           Boolean overwriteBody, JsonObject payload,
                                           Map<String, Object> headers, Map<String, Object> attributes) {

        ConnectorResponse response = new DefaultConnectorResponse();
        if (overwriteBody != null && overwriteBody) {
            org.apache.axis2.context.MessageContext axisMsgCtx = ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            String jsonString = payload.toString();
            try {
                JsonUtil.getNewJsonPayload(axisMsgCtx, jsonString, true, true);
            } catch (AxisFault e) {
                handleException("Error setting response payload", e, messageContext);
            }
            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, Const.CONTENT_TYPE_JSON);
            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, Const.CONTENT_TYPE_JSON);
        } else {
            String output = payload.toString();
            response.setPayload(output);
        }
        response.setHeaders(headers);
        response.setAttributes(attributes);
        messageContext.setVariable(responseVariable, response);
    }
}
