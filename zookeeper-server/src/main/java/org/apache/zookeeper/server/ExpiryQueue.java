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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */

/**
 * 在zookeeper服务端中需要管理session和connection这两类具有超时属性的对象。
 * zookeeper提供了ExpiryQueue来实现通用对象超时管理容器。
 */
public class ExpiryQueue<E> {

    //记录了每一个对象的归一化后的超时时间点，key是被管理的对象，value是超时时间点
    //  key的可能是 SessionImpl（大多数情况就是这个类）  ，也可能是 NIOServerCnxn
    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     */
    //存储了相同超时时间点的所有对象，key是超时时间点，value是相同超时时间点的对象集合
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong();

    /**
     *  不同的连接超时的时间点是不同的
     *[](/Image-Architecture/Zookeeper/image7.jpg)
     * zookeeper会使用expirationInterval作为基准把每一个连接的超时时间点归一化为expirationInterval整数倍，归一化的计算方式为
     * timeoutPoint =  CurrentTime+SessionTimeOut
     * normalizeTimeout = (timeoutPoint/expirationInterval +1) * expirationInterval
     */
    private final int expirationInterval; // 就是tickTime，tickTime默认是2000毫秒，所以expirationInterval默认也是2s

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    private long roundToNextInterval(long time) {
        /**
         * time就是系统的毫秒时间+sessionTimeout（默认好像是30s），expirationInterval就是tickTime，也就是心跳时间，默认是2000毫秒
         * 为什么要这样计算过期时间？ 直接 nowTime + timeout不就行了？
         * 如果一台zk  有100个客户端连接，也就是100个session，正常思路：zk会不断遍历这100个session，查看是否过期，这样效率太低了。
         * 这就引出zk的分桶策略：
         * 1、计算出过期时间 T = nowTime + timeout
         * 2、( T / expirationInterval + 1) * expirationInterval  跟 (T / expirationInterval) * expirationInterval 有啥区别
         * 先举个例子：  (3/2 + 1 ) * 2 = 4 > 3 ;  ( 3/2 ) * 2 = 2 < 3
         * 正常过期时间 肯定是 T = nowTime + timeout
         * ( T / expirationInterval + 1) * expirationInterval 这样做的目的就是 计算出一个过期时间 newT
         * 这个newT  < 1 > 首先肯定比 T 大，但是不会大很多 也就是说 newT 约等于 T
         *  (10/5 + 1 ) * 5 = 15;  (11/5 + 1 ) * 5 = 15 ; (12/5 + 1 ) * 5 = 15; (13/5 + 1 ) * 5 = 15 ..
         *   10、11、12、13、14  这4个过期时间就都在一个桶里，可以理解为他15就是一个桶。
         *  数据结构  ConcurrentHashMap<Long, Set<E>> expiryMap : key是超时时间点，value是相同超时时间点的对象集合
         *                                                 这样  ->  ZK在进行扫描的时候，只需要扫描一个桶即可
         *          ConcurrentHashMap<E, Long> elemMap  记录了每一个对象的归一化后的超时时间点，key是被管理的对象，value是超时时间点
         *
         *         < 2 > 然后 这个 newT是 expirationInterval 整数倍
         *         这样让每个 Session的过期时间 和 检查时间 在一个时间节点上。否则的话就会出现一个问题：ZK检查完毕的1毫秒后，
         *         就有一个Session新过期了，这种情况肯定是不好
         **/
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * @param elem     element to add/update
     * @param timeout  timout in milliseconds
     * @return time at which the element is now set to expire if
     *                 changed, or null if unchanged
     */
    // SesessionImpl    timeout是超时对象的超时时间（或者说存活时长）
    public Long update(E elem, int timeout) {  // 30s
        // 之前保存的session的过期时间点
        Long prevExpiryTime = elemMap.get(elem);
        long now = Time.currentElapsedTime();// 可以理解为获取的就是系统毫秒时间

        // 基于当前时间和设置的超时时间，以及ticktime ,计算出下一个过期时间
        // 比如当前时间 20s.111毫秒   timeout是10秒，那么过期时间不是30s.111毫秒，而是32s
        // 下一次过期的时间点
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // 如果过期时间没有变化，则不用更新
        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // expiryMap中保存的是某个过期时间（时间点）对应的session集合
        // First add the elem to the new expiry time bucket in expiryMap.
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            //如果超时对象集合为空，那么创建一个
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            //并发的情况下可能会出现多个线程同时创建相同超时时间点对象集合，所以需要做如下是否存在判断处理
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        //把本超时对象加入集合
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        //同时更新超时对象在elemMap中新的超时时间点
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            //根据超时对象上一个超时时间点从expiryMap对应的超时对象集合中把本超时对象删除
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        // 下个过期时间  xxxx .04s   02,,
        long expirationTime = nextExpirationTime.get();
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        // 02s
        long expirationTime = nextExpirationTime.get();   // <=now
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;

        // 06
        long newExpirationTime = expirationTime + expirationInterval; // expirationInterval就是tickTime

        // 先设置下一个过期时间
        if (nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {
            // 把处于当前已经过期的session移除出来
            set = expiryMap.remove(expirationTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }
        //返回在本次expirationTime超时时间点超时的对象
        return set;

        //每次在超时时间点获取超时对象之后，超时管理线程可以根据超时对象的不同业务特性做不同的业务逻辑
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -&gt; elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }

}

