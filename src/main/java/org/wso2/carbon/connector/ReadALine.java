/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.carbon.connector;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.Connector;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.FileConnectorUtils;
import org.wso2.carbon.connector.util.FileConstants;
import org.wso2.carbon.connector.util.ResultPayloadCreate;

/**
 * Reads a specific line from a file.
 */
public class ReadALine extends AbstractConnector implements Connector {
    private static final Log log = LogFactory.getLog(ReadALine.class);

    public void connect(MessageContext messageContext) {

        String fileLocation = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String lineNumber = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.LINE_NUMBER);

        FileObject fileObj = null;
        StandardFileSystemManager manager = null;
        try {
            manager = FileConnectorUtils.getManager();
            fileObj = manager.resolveFile(fileLocation, FileConnectorUtils.init(messageContext));
            if (!fileObj.exists() || fileObj.getType() != FileType.FILE) {
                log.error("File does not exists, or source is not a file.");
                handleException("File does not exists, or source is not a file.", messageContext);
            } else {
                if (StringUtils.isEmpty(lineNumber)) {
                    log.error("Line number is not provided to read.");
                    handleException("Line number is not provided to read.", messageContext);
                } else {
                    ResultPayloadCreate.readALine(fileObj, messageContext, lineNumber);
                }
            }
        } catch (FileSystemException e) {
            log.error("Error while processing the file/folder", e);
            throw new SynapseException("Error while processing the file/folder", e);
        } finally {
            if (fileObj != null) {
                try {
                    fileObj.close();
                } catch (FileSystemException e) {
                    log.error("Error while closing the sourceFileObj: " + e.getMessage(), e);
                }
            }
            if (manager != null) {
                //close the StandardFileSystemManager
                manager.close();
            }
        }
    }
}
