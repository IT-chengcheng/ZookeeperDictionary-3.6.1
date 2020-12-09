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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.WorkerService;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up. Instead of just waiting for the committed requests,
 * we process the uncommitted requests that belong to other sessions.
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS = "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT = "zookeeper.commitProcessor.shutdownTimeout";
    /** Default max read batch size: -1 to disable the feature */
    public static final String ZOOKEEPER_COMMIT_PROC_MAX_READ_BATCH_SIZE = "zookeeper.commitProcessor.maxReadBatchSize";
    /** Default max commit batch size: 1 */
    public static final String ZOOKEEPER_COMMIT_PROC_MAX_COMMIT_BATCH_SIZE = "zookeeper.commitProcessor.maxCommitBatchSize";

    /**
     * Incoming requests.
     */
    protected LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Incoming requests that are waiting on a commit,
     * contained in order of arrival
     * 保留着需要等待commit的请求
     */
    protected final LinkedBlockingQueue<Request> queuedWriteRequests = new LinkedBlockingQueue<>();

    /**
     * The number of read requests currently held in all session queues
     */
    private AtomicInteger numReadQueuedRequests = new AtomicInteger(0);

    /**
     * The number of quorum requests currently held in all session queued
     */
    private AtomicInteger numWriteQueuedRequests = new AtomicInteger(0);

    /**
     * Requests that have been committed.
     */
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Requests that we are holding until commit comes in. Keys represent
     * session ids, each value is a linked list of the session's requests.
     * 某个session中需要被提交的请求，按顺序存放
     */
    protected final Map<Long, Deque<Request>> pendingRequests = new HashMap<>(10000);

    /** The number of requests currently being processed */
    protected final AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    RequestProcessor nextProcessor;

    /** For testing purposes, we use a separated stopping condition for the
     * outer loop.*/
    protected volatile boolean stoppedMainLoop = true;
    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;
    private Object emptyPoolSync = new Object();

    /**
     * Max number of reads to process from queuedRequests before switching to
     * processing commits. If the value is negative, we switch whenever we have
     * a local write, and pending commits.
     * A high read batch size will delay commit processing causing us to
     * serve stale data.
     */
    private static volatile int maxReadBatchSize;
    /**
     * Max number of commits to process before processing reads. We will try to
     * process as many remote/local commits as we can till we reach this
     * count.
     * A high commit batch size will delay reads while processing more commits.
     * A low commit batch size will favor reads.
     */
    private static volatile int maxCommitBatchSize;

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
        case OpCode.create:
        case OpCode.create2:
        case OpCode.createTTL:
        case OpCode.createContainer:
        case OpCode.delete:
        case OpCode.deleteContainer:
        case OpCode.setData:
        case OpCode.reconfig:
        case OpCode.multi:
        case OpCode.setACL:
        case OpCode.check:
            return true;
        case OpCode.sync:
            return matchSyncs;
        case OpCode.createSession:
        case OpCode.closeSession:
            return !request.isLocalSession(); // 不是Local则是true
        default:
            return false;
        }
    }

    @Override
    public void run() {
        try {
            /*
             * In each iteration of the following loop we process at most
             * requestsToProcess requests of queuedRequests. We have to limit
             * the number of request we poll from queuedRequests, since it is
             * possible to endlessly poll read requests from queuedRequests, and
             * that will lead to a starvation of non-local committed requests.
             */
            int requestsToProcess = 0;
            boolean commitIsWaiting = false;
            do {
                /*
                 * Since requests are placed in the queue before being sent to
                 * the leader, if commitIsWaiting = true, the commit belongs to
                 * the first update operation in the queuedRequests or to a
                 * request from a client on another server (i.e., the order of
                 * the following two lines is important!).
                 */

                // committedRequests不为空，表示已经有请求被提交了(已经向follower节点发送了commit命令，leader自己还没有提交这个请求)
                // 存在需要生效的请求（更新DataBase） //nextrequestProcessor
                commitIsWaiting = !committedRequests.isEmpty();

                // queuedRequests表示待处理的请求（包括读请求和写请求）
                // 包含了当前服务接收到的所有请求
                // committedRequests队列中的请求正常情况下在queuedRequests队列中都存在
                requestsToProcess = queuedRequests.size();


                // 没有待处理的请求，并且也没有等待被提交的请求，那就阻塞等待
                // Avoid sync if we have something to do
                if (requestsToProcess == 0 && !commitIsWaiting) {
                    // Waiting for requests to process
                    synchronized (this) {
                        while (!stopped && requestsToProcess == 0 && !commitIsWaiting) {
                            wait();
                            // 被唤醒之后，queuedRequests添加了数据或者committedRequests添加了数据
                            commitIsWaiting = !committedRequests.isEmpty();   // Leader节点提交了请求，或Follower节点接收到了Commit命令
                            requestsToProcess = queuedRequests.size();   // 接收到了新请求
                        }
                    }
                }

                ServerMetrics.getMetrics().READS_QUEUED_IN_COMMIT_PROCESSOR.add(numReadQueuedRequests.get());
                ServerMetrics.getMetrics().WRITES_QUEUED_IN_COMMIT_PROCESSOR.add(numWriteQueuedRequests.get());
                ServerMetrics.getMetrics().COMMITS_QUEUED_IN_COMMIT_PROCESSOR.add(committedRequests.size());

                long time = Time.currentElapsedTime();

                /*
                 * Processing up to requestsToProcess requests from the incoming
                 * queue (queuedRequests). If maxReadBatchSize is set then no
                 * commits will be processed until maxReadBatchSize number of
                 * reads are processed (or no more reads remain in the queue).
                 * After the loop a single committed request is processed if
                 * one is waiting (or a batch of commits if maxCommitBatchSize
                 * is set).
                 */
                Request request;
                int readsProcessed = 0;

                // 这个循环的作用是不断的从queuedRequests中获取待处理请求进行判断
                // 如果请求是读请求，就直接交给nextProcessor进行处理，无需做额外的事情
                // 如果请求是写请求，就把该请求添加到pendingRequests中（相当于阻塞）
                // 什么时候退出这个while? 如果maxReadBatchSize=-1，那么接收到committedRequests就退出while, 如果maxReadBatchSize > -1, 当处理的读请求的数量超过了maxReadBatchSize才会退出while
                // 或者从queuedRequests中没有拿到数据，也会退出while

                // 所以这个方法的逻辑就是：不停的从queuedRequests中获取待处理的请求，如果是写请求则放入pendingRequests中，如果是读请求则判断对应的pendingRequests中是否存在还没提交的写请求
                // 并且还提供了一个功能，如果maxReadBatchSize设置了值，那么这里会等处理完这maxReadBatchSize多个读请求之后再去处理写请求

                // 如果一个客户端发了写请求，其他客户端在不断的发读请求，那么通过配置maxReadBatchSize可以让读优先
                while (!stopped
                       && requestsToProcess > 0
                        // 当处理的读请求数量超过了maxReadBatchSize，就暂时不会从queuedRequests中获取请求进行处理了
                       && (maxReadBatchSize < 0 || readsProcessed <= maxReadBatchSize)   // 10
                       && (request = queuedRequests.poll()) != null) {


                    requestsToProcess--;

                    // 当前请求是否是需要进行两阶段提交的请求，或者当前request对应的session已经有其他请求了
                    // 如果某一个请求是读请求，但是这个请求对应的session在之前有还没提交的写请求，那么该读请求也得加到pendingRequests
                    if (needCommit(request) || pendingRequests.containsKey(request.sessionId)) {   // key存在，则key所对应的队列中有数据

                        // 添加请求到sessionid对应的队列中
                        // Add request to pending
                        Deque<Request> requests = pendingRequests.computeIfAbsent(request.sessionId, sid -> new ArrayDeque<>());
                        requests.addLast(request);
                        ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(requests.size());
                    } else {
                        // 如果不是写请求，就这个请求就无需进行进行两阶段了，直接调用nextProcessor
                        readsProcessed++; // 读请求数量+1
                        numReadQueuedRequests.decrementAndGet();
                        // 内部是把这个请求交给sessionId对应的线程池进行处理的
                        sendToNextProcessor(request);  // nextProcess
                    }

                    /*
                     * Stop feeding the pool if there is a local pending update
                     * and a committed request that is ready. Once we have a
                     * pending request with a waiting committed request, we know
                     * we can process the committed one. This is because commits
                     * for local requests arrive in the order they appeared in
                     * the queue, so if we have a pending request and a
                     * committed request, the committed request must be for that
                     * pending write or for a write originating at a different
                     * server. We skip this if maxReadBatchSize is set.
                     */
                    // 如果没有限制最大读请求的数量，那么只要存在待处理请求和待提交请求就break出当前循环，就会出处理请求和提交请求
                    if (maxReadBatchSize < 0 && !pendingRequests.isEmpty() && !committedRequests.isEmpty()) {
                        /*
                         * We set commitIsWaiting so that we won't check
                         * committedRequests again.
                         */
                        // 表示一定是有待提交的请求
                        commitIsWaiting = true;
                        break;
                    }
                }
                ServerMetrics.getMetrics().READS_ISSUED_IN_COMMIT_PROC.add(readsProcessed);


                if (!commitIsWaiting) {
                    commitIsWaiting = !committedRequests.isEmpty();
                }

                /*
                 * Handle commits, if any.
                 */
                // 有待提交的请求
                if (commitIsWaiting && !stopped) {
                    /*
                     * Drain outstanding reads
                     */
                    waitForEmptyPool();

                    if (stopped) {
                        return;
                    }

                    //
                    int commitsToProcess = maxCommitBatchSize;

                    /*
                     * Loop through all the commits, and try to drain them.
                     */
                    Set<Long> queuesToDrain = new HashSet<>();
                    long startWriteTime = Time.currentElapsedTime();
                    int commitsProcessed = 0;

                    // 存在需要提交的请求
                    // 什么时候退出这个while：
                    // 1. 没有要提交的请求了
                    // 2. 或者有要提交的请求了，但是commitsToProcess<=0了，意思就是说已经连续处理了很多个写请求了，得去处理读请求了
                    // commitsToProcess默认为1，表示处理一个写请求后，就去处理读请求了
                    while (commitIsWaiting && !stopped && commitsToProcess > 0) {   // 10  xie

                        // 从committedRequests队列中获得一个请求（队头），拿到的这个请求表示两阶段提交已经执行完了，只要本地提交就可以了
                        // Process committed head
                        request = committedRequests.peek();

                        /*
                         * Check if this is a local write request is pending,
                         * if so, update it with the committed info. If the commit matches
                         * the first write queued in the blockedRequestQueue, we know this is
                         * a commit for a local write, as commits are received in order. Else
                         * it must be a commit for a remote write.
                         */
                        // committedRequests中的请求是其他服务器触发的，所以要和我本地保存的写请求顺序一致
                        if (!queuedWriteRequests.isEmpty()
                            && queuedWriteRequests.peek().sessionId == request.sessionId
                            && queuedWriteRequests.peek().cxid == request.cxid) {


                            /*
                             * Commit matches the earliest write in our write queue.
                             */
                            // 拿到当前请求对应的sessionId对应的请求队列
                            Deque<Request> sessionQueue = pendingRequests.get(request.sessionId);
                            ServerMetrics.getMetrics().PENDING_SESSION_QUEUE_SIZE.add(pendingRequests.size());
                            if (sessionQueue == null || sessionQueue.isEmpty() || !needCommit(sessionQueue.peek())) {
                                /*
                                 * Can't process this write yet.
                                 * Either there are reads pending in this session, or we
                                 * haven't gotten to this write yet.
                                 */
                                break;
                            } else {
                                ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(sessionQueue.size());
                                // If session queue != null, then it is also not empty.
                                // 从sessionQueue移除请求
                                Request topPending = sessionQueue.poll();
                                /*
                                 * Generally, we want to send to the next processor our version of the request,
                                 * since it contains the session information that is needed for post update processing.
                                 * In more details, when a request is in the local queue, there is (or could be) a client
                                 * attached to this server waiting for a response, and there is other bookkeeping of
                                 * requests that are outstanding and have originated from this server
                                 * (e.g., for setting the max outstanding requests) - we need to update this info when an
                                 * outstanding request completes. Note that in the other case, the operation
                                 * originated from a different server and there is no local bookkeeping or a local client
                                 * session that needs to be notified.
                                 */
                                topPending.setHdr(request.getHdr());
                                topPending.setTxn(request.getTxn());
                                topPending.setTxnDigest(request.getTxnDigest());
                                topPending.zxid = request.zxid;
                                topPending.commitRecvTime = request.commitRecvTime;
                                request = topPending;
                                // Only decrement if we take a request off the queue.
                                numWriteQueuedRequests.decrementAndGet();
                                // 从queuedWriteRequests队列中移除
                                queuedWriteRequests.poll();

                                queuesToDrain.add(request.sessionId);
                            }
                        }


                        /*
                         * Pull the request off the commit queue, now that we are going
                         * to process it.
                         */
                        committedRequests.remove();
                        commitsToProcess--;
                        commitsProcessed++;

                        // Process the write inline.
                        // 处理写请求，交给下一个nextProcessor
                        processWrite(request);

                        commitIsWaiting = !committedRequests.isEmpty();
                    }

                    // 处理完写请求后,就可以去这个写请求对应的SessionID的队列中的后续的请求了（可能是读、写请求）

                    ServerMetrics.getMetrics().WRITE_BATCH_TIME_IN_COMMIT_PROCESSOR
                        .add(Time.currentElapsedTime() - startWriteTime);
                    ServerMetrics.getMetrics().WRITES_ISSUED_IN_COMMIT_PROC.add(commitsProcessed);

                    /*
                     * Process following reads if any, remove session queue(s) if
                     * empty.
                     */
                    readsProcessed = 0;
                    for (Long sessionId : queuesToDrain) {
                        //
                        Deque<Request> sessionQueue = pendingRequests.get(sessionId);

                        int readsAfterWrite = 0;

                        // 循环当前sessionId对应的请求队列
                        while (!stopped && !sessionQueue.isEmpty() && !needCommit(sessionQueue.peek())) {
                            numReadQueuedRequests.decrementAndGet();
                            sendToNextProcessor(sessionQueue.poll());
                            readsAfterWrite++;
                        }

                        ServerMetrics.getMetrics().READS_AFTER_WRITE_IN_SESSION_QUEUE.add(readsAfterWrite);
                        // 一个写请求后的读请求
                        readsProcessed += readsAfterWrite;

                        // 移除
                        // Remove empty queues
                        if (sessionQueue.isEmpty()) {
                            pendingRequests.remove(sessionId);
                        }
                    }
                    ServerMetrics.getMetrics().SESSION_QUEUES_DRAINED.add(queuesToDrain.size());
                    ServerMetrics.getMetrics().READ_ISSUED_FROM_SESSION_QUEUE.add(readsProcessed);
                }

                ServerMetrics.getMetrics().COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - time);
                endOfIteration();
            } while (!stoppedMainLoop);
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    //for test only
    protected void endOfIteration() {

    }

    protected void waitForEmptyPool() throws InterruptedException {
        int numRequestsInProcess = numRequestsProcessing.get();
        if (numRequestsInProcess != 0) {
            ServerMetrics.getMetrics().CONCURRENT_REQUEST_PROCESSING_IN_COMMIT_PROCESSOR.add(numRequestsInProcess);
        }

        long startWaitTime = Time.currentElapsedTime();
        synchronized (emptyPoolSync) {
            while ((!stopped) && isProcessingRequest()) {
                emptyPoolSync.wait();
            }
        }
        ServerMetrics.getMetrics().TIME_WAITING_EMPTY_POOL_IN_COMMIT_PROCESSOR_READ
            .add(Time.currentElapsedTime() - startWaitTime);
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        initBatchSizes();

        LOG.info(
            "Configuring CommitProcessor with {} worker threads.",
            numWorkerThreads > 0 ? numWorkerThreads : "no");
        if (workerPool == null) {
            workerPool = new WorkerService("CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        stoppedMainLoop = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    private void processWrite(Request request) throws RequestProcessorException {
        processCommitMetrics(request, true);

        long timeBeforeFinalProc = Time.currentElapsedTime();
        nextProcessor.processRequest(request);
        ServerMetrics.getMetrics().WRITE_FINAL_PROC_TIME.add(Time.currentElapsedTime() - timeBeforeFinalProc);
    }

    private static void initBatchSizes() {
        maxReadBatchSize = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_MAX_READ_BATCH_SIZE, -1);
        maxCommitBatchSize = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_MAX_COMMIT_BATCH_SIZE, 1);

        if (maxCommitBatchSize <= 0) {
            String errorMsg = "maxCommitBatchSize must be positive, was " + maxCommitBatchSize;
            throw new IllegalArgumentException(errorMsg);
        }

        LOG.info
            ("Configuring CommitProcessor with readBatchSize {} commitBatchSize {}",
             maxReadBatchSize,
             maxCommitBatchSize);
    }

    private static void processCommitMetrics(Request request, boolean isWrite) {
        if (isWrite) {
            if (request.commitProcQueueStartTime != -1 && request.commitRecvTime != -1) {
                // Locally issued writes.
                long currentTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().WRITE_COMMITPROC_TIME.add(currentTime - request.commitProcQueueStartTime);
                ServerMetrics.getMetrics().LOCAL_WRITE_COMMITTED_TIME.add(currentTime - request.commitRecvTime);
            } else if (request.commitRecvTime != -1) {
                // Writes issued by other servers.
                ServerMetrics.getMetrics().SERVER_WRITE_COMMITTED_TIME
                    .add(Time.currentElapsedTime() - request.commitRecvTime);
            }
        } else {
            if (request.commitProcQueueStartTime != -1) {
                ServerMetrics.getMetrics().READ_COMMITPROC_TIME
                    .add(Time.currentElapsedTime() - request.commitProcQueueStartTime);
            }
        }
    }

    public static int getMaxReadBatchSize() {
        return maxReadBatchSize;
    }

    public static int getMaxCommitBatchSize() {
        return maxCommitBatchSize;
    }

    public static void setMaxReadBatchSize(int size) {
        maxReadBatchSize = size;
        LOG.info("Configuring CommitProcessor with readBatchSize {}", maxReadBatchSize);
    }

    public static void setMaxCommitBatchSize(int size) {
        if (size > 0) {
            maxCommitBatchSize = size;
            LOG.info("Configuring CommitProcessor with commitBatchSize {}", maxCommitBatchSize);
        }
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {

        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor, unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                processCommitMetrics(request, needCommit(request));

                long timeBeforeFinalProc = Time.currentElapsedTime();
                nextProcessor.processRequest(request);
                if (needCommit(request)) {
                    ServerMetrics.getMetrics().WRITE_FINAL_PROC_TIME
                        .add(Time.currentElapsedTime() - timeBeforeFinalProc);
                } else {
                    ServerMetrics.getMetrics().READ_FINAL_PROC_TIME
                        .add(Time.currentElapsedTime() - timeBeforeFinalProc);
                }

            } finally {

                if (numRequestsProcessing.decrementAndGet() == 0) {
                    wakeupOnEmpty();
                }
            }
        }

    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private synchronized void wakeup() {
        notifyAll();
    }

    private void wakeupOnEmpty() {
        synchronized (emptyPoolSync) {
            emptyPoolSync.notifyAll();
        }
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        LOG.debug("Committing request:: {}", request);
        request.commitRecvTime = Time.currentElapsedTime();
        ServerMetrics.getMetrics().COMMITS_QUEUED.add(1);
        // request已经被提交（已经向follower节点发送了commit命令）
        committedRequests.add(request);
        // 唤醒被阻塞线程
        wakeup();
    }

    // 这个方法会被Leader、Follower、Observer三个角色都调用到
    //
    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        LOG.debug("Processing request:: {}", request);
        request.commitProcQueueStartTime = Time.currentElapsedTime();

        queuedRequests.add(request);

        // If the request will block, add it to the queue of blocking requests
        if (needCommit(request)) {
            // 如果是写请求，则需要进行两阶段提交
            queuedWriteRequests.add(request);
            numWriteQueuedRequests.incrementAndGet();
        } else {
            numReadQueuedRequests.incrementAndGet();
        }
        // 唤醒
        wakeup();
    }

    private void halt() {
        stoppedMainLoop = true;
        stopped = true;
        wakeupOnEmpty();
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
