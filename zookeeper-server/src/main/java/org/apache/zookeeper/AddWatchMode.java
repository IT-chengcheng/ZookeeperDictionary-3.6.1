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

package org.apache.zookeeper;

/**
 * Modes available to {@link ZooKeeper#addWatch(String, Watcher, AddWatchMode)}
 */
public enum AddWatchMode {
    /**
     * <p>
     * Set a watcher on the given path that does not get removed when triggered (i.e. it stays active
     * until it is removed). This watcher
     * is triggered for both data and child events. To remove the watcher, use
     * <tt>removeWatches()</tt> with <tt>WatcherType.Any</tt>. The watcher behaves as if you placed an exists() watch and
     * a getData() watch on the ZNode at the given path.
     * </p>
     */
    // 之前的版本，一个watch被触发一次之后，就会被取消掉，而PERSISTENT类型的Watch就不会被取消掉了
    PERSISTENT(ZooDefs.AddWatchModes.persistent),

    /**
     * <p>
     * Set a watcher on the given path that: a) does not get removed when triggered (i.e. it stays active
     * until it is removed); b) applies not only to the registered path but all child paths recursively. This watcher
     * is triggered for both data and child events. To remove the watcher, use
     * <tt>removeWatches()</tt> with <tt>WatcherType.Any</tt>
     * </p>
     *
     * <p>
     * The watcher behaves as if you placed an exists() watch and
     * a getData() watch on the ZNode at the given path <strong>and</strong> any ZNodes that are children
     * of the given path including children added later.
     * </p>
     *
     * <p>
     * NOTE: when there are active recursive watches there is a small performance decrease as all segments
     * of ZNode paths must be checked for watch triggering.
     * </p>
     */
    // 递归，子节点的数据变化也会触发Watcher，而且子节点的子节点数据发生变化也会触发监听器
    PERSISTENT_RECURSIVE(ZooDefs.AddWatchModes.persistentRecursive);

    public int getMode() {
        return mode;
    }

    private final int mode;

    AddWatchMode(int mode) {
        this.mode = mode;
    }
}
