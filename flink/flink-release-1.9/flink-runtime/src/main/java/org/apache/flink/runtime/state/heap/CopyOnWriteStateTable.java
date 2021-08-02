/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

/**
 * This implementation of {@link StateTable} uses {@link CopyOnWriteStateMap}. This implementation supports asynchronous snapshots.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
/**
	当使用 MemoryStateBackend 和 FsStateBackend 时，默认情况下会将状态数据保存到 CopyOnWriteStateTable 中。
	CopyOnWriteStateTable 中保存多个 KeyGroup 的状态，每个 KeyGroup 对应一个 CopyOnWriteStateMap。

    todo 该类主要用CopyOnWriteStateMap 存储数据 CopyOnWriteStateMap是一个数组+链表构成的hash表
 */
public class CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S> {

	/**
	 * Constructs a new {@code CopyOnWriteStateTable}.
	 *
	 * @param keyContext    the key context.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 */
	CopyOnWriteStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
	}

	/**
	 * CopyOnWriteStateMap 是一个类似于 HashMap 的结构，但支持了两个非常有意思的功能：
	 *
	 * 1、hash 结构为了保证读写数据的高性能，都需要有扩容策略，CopyOnWriteStateMap 的扩容策略是一个渐进式 rehash 的策略，
	 * 即：不是一下子将数据全迁移的新的 hash 表，而是慢慢去迁移数据到新的 hash 表中。
	 * 2、Checkpoint 时 CopyOnWriteStateMap 支持异步快照，即：Checkpoint 时可以在做快照的同时，
	 * 仍然对 CopyOnWriteStateMap 中数据进行修改。问题来了：数据修改了，怎么保证快照数据的准确性呢？
	 */
	@Override
	protected CopyOnWriteStateMap<K, N, S> createStateMap() {
		return new CopyOnWriteStateMap<>(getStateSerializer());
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link CopyOnWriteStateTable}, to be written in checkpointing.
	 *
	 * @return a snapshot from this {@link CopyOnWriteStateTable}, for checkpointing.
	 */
	@Nonnull
	@Override
	public CopyOnWriteStateTableSnapshot<K, N, S> stateSnapshot() {
		return new CopyOnWriteStateTableSnapshot<>(
			this,
			getKeySerializer().duplicate(),
			getNamespaceSerializer().duplicate(),
			getStateSerializer().duplicate(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	@SuppressWarnings("unchecked")
	List<CopyOnWriteStateMapSnapshot<K, N, S>> getStateMapSnapshotList() {
		List<CopyOnWriteStateMapSnapshot<K, N, S>> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
		for (int i = 0; i < keyGroupedStateMaps.length; i++) {
			CopyOnWriteStateMap<K, N, S> stateMap = (CopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[i];
			snapshotList.add(stateMap.stateSnapshot());
		}
		return snapshotList;
	}
}
