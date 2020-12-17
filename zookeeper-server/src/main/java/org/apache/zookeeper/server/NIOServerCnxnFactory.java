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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 *   - 1   accept thread, which accepts new connections and assigns to a
 *         selector thread
 *   - 1-N selector threads, each of which selects on 1/N of the connections.
 *         The reason the factory supports more than one selector thread is that
 *         with large numbers of connections, select() itself can become a
 *         performance bottleneck.
 *   - 0-M socket I/O worker threads, which perform basic socket reads and
 *         writes. If configured with 0 worker threads, the selector threads
 *         do the socket I/O directly.
 *   - 1   connection expiration thread, which closes idle connections; this is
 *         necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /** Default sessionless connection timeout in ms: 10000 (10s) */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT = "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS = "zookeeper.nio.numSelectorThreads";
    /** Default: 2 * numCores */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS = "zookeeper.nio.numWorkerThreads";
    /** Default: 64kB */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES = "zookeeper.nio.directBufferBytes";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT = "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread {} died", t, e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch (IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {

        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            // 居然是在这里，open（）的
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close.", e);
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    LOG.debug("ignoring exception during selectionkey cancel", ex);
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }

    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */
    private class AcceptThread extends AbstractSelectThread {

        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr, Set<SelectorThread> selectorThreads) throws IOException {
            /**
             * 执行父类AbstractSelectThread  构造方法方法 打开selector ->  Selector.open()
             */
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            //  往ServerSocketChannel注册 selector
            this.acceptKey = acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(new ArrayList<SelectorThread>(selectorThreads));
            // 这个selectorThreads线程数组，是外面通过CPU的核数计算出来的
            selectorIterator = this.selectorThreads.iterator();
        }

        public void run() {
            try {
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    // 连接事件
                    if (key.isAcceptable()) {
                        // 接收一个socket连接
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * Accept new socket connections. Enforces maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                sc = acceptSocket.accept();
                accepted = true;
                if (limitTotalNumberOfCnxns()) {
                    throw new IOException("Too many connections max allowed is " + maxCnxns);
                }
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);

                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from {}", sc.socket().getRemoteSocketAddress());

                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                // 每接收到一个socket连接，就以轮询的方式把当前socket连接交给一个selectorThread去处理该socket
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                SelectorThread selectorThread = selectorIterator.next();

                // 把当前sc添加到selectorThread的队列中，selectorThread会处理它自己的队列中的sc，到时候会取出来注册读写事件
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException("Unable to add connection to selector queue"
                                          + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);
                acceptErrorLogger.rateLimitLog("Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }

    }

    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     *
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     *
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     *
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */
    class SelectorThread extends AbstractSelectThread {

        private final int id;
        /**
         * AcceptThread 把具有连接事件的 socketchannal 都放到SelectorThread这个acceptedQueue队列中，
         * 然后在processAcceptedConnections取出来注册读写事件
         */
        private final Queue<SocketChannel> acceptedQueue;
        /**
         * 是在这里加的 IOWorkRequest -> doWork() 方法块的最后  -> selectorThread.addInterestOpsUpdateRequest(SelectionKey)
         * 也就是处理完一个事件后，把这个 SelectionKey加入队列中，为了以后再取出来，重新注册感兴趣的事件
         */
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        public void run() {
            try {

                while (!stopped) {
                    try {
                        /**
                         * 真正的关键方法 执行了 selector.select()！！！！！！
                         */
                        select();

                        /**
                         * 这个方法会对acceptedQueue队列中的sc向select上注册OP_READ事件，并且给SelectionKey添加attachment
                         * 也就是执行  socketChannel.register(selector,SelectionKey.OP_READ);
                         * acceptedQueue队列中的sc是啥时候加进去的？ 是在 AcceptThread 的 run（）方法里面（一步步看代码）
                         */
                        processAcceptedConnections();
                        /**
                         * 这个方法就是重新对  updateQueue 队列中的socketchanel注册感兴趣的事件
                         */
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    }
                    cleanupSelectionKey(key);
                }

                //
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                // 这个selector 是在 SelectorThread 的父类 AbstractSelectThread中 selector = Selector.open()
                selector.select();
                // 有就绪事件
                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);

                // 这里会打乱就绪事件，所以事件的顺序是乱的
                Collections.shuffle(selectedList);

                // 遍历就绪事件
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    // 读就绪事件或写就绪事件
                    if (key.isReadable() || key.isWritable()) {
                        // 处理客户端的命令、请求
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void handleIO(SelectionKey key) {
            // 把当前要处理的key封装为一个IOWorkRequest对象，等下交给workerPool进行调度处理
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its
            // connection
            // 处理完一个key才去处理下一个key
            cnxn.disableSelectable();

            /**
             * 1、 socketChannel在selector注册对任何事件都不感兴趣
             * zookeeper对单个连接上的IO事件是按照顺序一个一个处理的，它是如何实现按照IO事件发生顺序一个一个处理的呢？
             * 当socketChannel接受到一个IO事件之后，他就会设置对任何事件都不感兴趣，这样后来到来的事件就
             * 没有办法传递给server端的socketChannel，
             * 2、那么在一个IO事件处理完成之后，socketChannel是如何再次向selector注册感兴趣的事件的呢？
             *    这个就是processInterestOpsUpdateRequests（）的工作了
             */
            key.interestOps(0);

            // 更新NIOServerCnxn上的时间，NIOServerCnxn也是一个对象，如果一直没有接收到数据（客户端一直没有发送数据和ping），
            // 就需要把这个对象删掉
            // 续期
            touchCnxn(cnxn);

            // 处理workRequest
            workerPool.schedule(workRequest);
        }

        /**
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            /**
             * 注意 acceptedQueue.poll() 是 Queue接口的方法，这个接口没有take（）方法
             * acceptedQueue.take() 是  LinkedBlockingQueue类中的方法
             * 这两个方法是有区别的，一个阻塞，一个不阻塞
             * acceptedQueue里面的 SocketChannel啥时候加进去的？是在AcceptThread里面加的，在这个方法里就是取出来，注册读事件
             */
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    // 一个sc对应一个NIOServerCnxn,socket连接的上下文，这个类也很重要
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    key.attach(cnxn);
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /** resume /rɪˈzjuːm/重新开始
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            /**
             * 首先看一下英文注释，然后注意 updateQueue 与  acceptedQueue 这两个队列的用法
             * updateQueue 队列的值  是在处理完一个事件后，加进去的。这里就是重新注册感兴趣的事件
             * 是在这里加的 IOWorkRequest -> doWork() 方法块的最后  -> selectorThread.addInterestOpsUpdateRequest(SelectionKey)
             */
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }

    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {

        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn; // 当前key对应的是哪个socketChannel

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {


            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            // 读就绪或写就绪
            if (key.isReadable() || key.isWritable()) {

                /**
                 * 处理key，也就是处理各种命令请求，到这里，多个客户端请求还是并发处理的
                 * 处理 增删查改
                 */
                cnxn.doIO(key); // 顺序

                // Check if we shutdown or doIO() closed this connection
                if (stopped) {
                    cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    return;
                }
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close(ServerCnxn.DisconnectReason.CONNECTION_MODE_CHANGED);
            }
        }

        @Override
        public void cleanup() {
            cnxn.close(ServerCnxn.DisconnectReason.CLEAN_UP);
        }

    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {

        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        ServerMetrics.getMetrics().SESSIONLESS_CONNECTIONS_EXPIRED.add(1);
                        conn.close(ServerCnxn.DisconnectReason.CONNECTION_EXPIRED);
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }

    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(directBufferBytes);
        }
    };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;
    int listenBacklog = -1;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;

    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();

    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();

        maxClientCnxns = maxcc;
        // 调用的是父类的方法
        initMaxCnxns();
        sessionlessCnxnTimeout = Integer.getInteger(ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue = new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();
       // 获取CPU的核数
        int numCores = Runtime.getRuntime().availableProcessors();
        // 32 cores sweet spot seems to be 4 selector threads
        // Math.sqrt（25）就是求25的平方根，也就是5，就像上面的英文注释 32 除以 2 = 16，再求平方根 就是 4
        numSelectorThreads = Integer.getInteger(
            ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
            Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        numWorkerThreads = Integer.getInteger(ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        String logMsg = "Configuring NIO connection handler with "
            + (sessionlessCnxnTimeout / 1000) + "s sessionless connection timeout, "
            + numSelectorThreads + " selector thread(s), "
            + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads, and "
            + (directBufferBytes == 0 ? "gathered writes." : ("" + (directBufferBytes / 1024) + " kB direct buffers."));
        LOG.info(logMsg);
        for (int i = 0; i < numSelectorThreads; ++i) {
            // 最后会将这个 selector线程数组 传给 AcceptThread ，它里面有个set集合保存
            selectorThreads.add(new SelectorThread(i));
        }
        //listenBacklog 用来设置socket连接队列的最大值
        listenBacklog = backlog;
        /**
         * 这是才是真正要启动NioSocketServer了，当然也仅仅是开启连接和绑定，还没有执行如下两个方法
         *  selector.select(timeOut);
         *  Set<SelectionKey> selectionKeys = selector.selectedKeys()
         */
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port {}", addr);
        if (listenBacklog == -1) {
            ss.socket().bind(addr);
        } else {
            ss.socket().bind(addr, listenBacklog);
        }
        ss.configureBlocking(false);
        /**
         * 这是zookeeper自定义线程，里面做了很多操作，比如注册selector ->
         * ServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
         */
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port.", e);
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port {}", addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {}", addr, e);
            tryClose(oldSS);
        }
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    /** {@inheritDoc} */
    public int getSocketListenBacklog() {
        return listenBacklog;
    }

    @Override
    public void start() {
        stopped = false;

        // 工作线程池
        if (workerPool == null) {
            /**
             * 注意第三个参数传false，意思是不指定创建线程池的数量。
             * numWorkerThreads在configure（）赋值，默认是CPU核心数乘以 2。也就是创建2个线程池
             * 此构造方法里面创建了numWorkerThreads个线程池，放入到属性 ArrayList<ExecutorService> workers
             */
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }

        // 启动selector线程，selectorThreads创建是在config（）方法，数量默认是：CPU核心数除以2，再开平方根
        for (SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                /**
                 * 启动线程，线程的run（）方法里就开始真正的 接收 connect read等等
                 */
                thread.start();
            }
        }

        // ensure thread is started once and only once
        // 启动接收连接线程 SocketChannel
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }

        // 暂时没有研究
        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    // 绑定端口，以便接收客户端请求
    @Override
    public void startup(ZooKeeperServer zks, boolean startServer) throws IOException, InterruptedException {
        // 1. 初始化WorkerService 线程池
        // 2. 启动SelectorThread，负责接收读写就绪事件
        // 3. 启动AcceptThread， 负责接收连接事件
        start();

        // 让zookeeperServer 跟 这个类 互相持有
        setZooKeeperServer(zks);

        if (startServer) {
            /** 很重要的一个方法，里面做了很多事  zxid
             *  初始化ZKDatabase，加载数据
             * 这里还会把之前的session给加载出来，放入到sessionsWithTimeouts中
             */
            zks.startdata();

            // 1. 创建sessionTracker
            // 2. 初始化RequestProcessor Chain
            // 3. 创建requestThrottler
            // 4. 注册jmx
            // 5. 修改为RUNNING状态
            // 6. notifyAll(), 因为上面的步骤中会启动线程，那些线程在运行的过程中如果发现一些其他的前置条件还没有满足，则会wait，而此处就会notify
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        removeCnxnFromSessionMap(cnxn);

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * Add or update cnxn in our cnxnExpiryQueue
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) {
            return 0;
        }
        return s.size();
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll(ServerCnxn.DisconnectReason reason) {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close(reason);
            } catch (Exception e) {
                LOG.warn(
                    "Ignoring exception closing cnxn session id 0x{}",
                    Long.toHexString(cnxn.getSessionId()),
                    e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            if (acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }

}
