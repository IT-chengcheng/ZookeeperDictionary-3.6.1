/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for performing local session upgrade. Only request submitted
 * directly to the leader should go through this processor.
 */
public class LeaderRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderRequestProcessor.class);

    private final LeaderZooKeeperServer lzks;

    private final RequestProcessor nextProcessor;

    public LeaderRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.lzks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void processRequest(Request request) throws RequestProcessorException {
        // Screen quorum requests against ACLs first
        if (!lzks.authWriteRequest(request)) {
            return;
        }

        // Check if this is a local session and we are trying to create
        // an ephemeral node, in which case we upgrade the session
        // 检查是不是一个创建临时节点的请求，如果是，则检查该临时节点对应的sessionId对应的session存不存在
        // 如果不存在，则要创建一个session

        // 这里就涉及到LocalSession和GloabSession
        // 本地Session表示：客户端与当前服务器节点创建的Session，只存在在当前服务器内
        // 全局Session表示：客户端与某个服务器节点创建的Session，会同步给其他服务器
        Request upgradeRequest = null;
        try {
            // 如果本地Session打开了，那么创建Session的动作是不会同步给其他服务器的
            // 这种情况下，如果Leader接收到了一个创建临时节点的请求（对于Leader节点而言，该临时节点是可以创建的（本地有该Session））
            // 但是，如果把创建临时节点的请求同步给其他服务器，其他服务器上是没有该Session的，所以需要构造一个创建Session的Request出来
            // 并且该请求的LocalSession标记得为false，才能同步给其他服务器
            upgradeRequest = lzks.checkUpgradeSession(request);
        } catch (KeeperException ke) {
            if (request.getHdr() != null) {
                LOG.debug("Updating header");
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(ke.code().intValue()));
            }
            request.setException(ke);
            LOG.warn("Error creating upgrade request", ke);
        } catch (IOException ie) {
            LOG.error("Unexpected error in upgrade", ie);
        }
        if (upgradeRequest != null) {
            nextProcessor.processRequest(upgradeRequest);
        }

        nextProcessor.processRequest(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
    }

}
