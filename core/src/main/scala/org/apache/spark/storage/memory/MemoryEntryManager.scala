/*
 * Created by Marvin on 11/2/16.
 */

package org.apache.spark.storage.memory

import java.util
import java.util.Map
import java.util.Map.Entry


private[storage] trait BaseMemoryManager[K, V] {
  def get(blockId: K): V
  def put(blockId: K, block: V): V
  def remove(blockId: K): V
  def clear(): Unit
  def contains(blockId: K): Boolean
  def entrySet(): util.Set[util.Map.Entry[K, V]]
}

private[storage] class LRUMemoryManager[K, V] extends BaseMemoryManager[K, V] {
  private val entries = new util.LinkedHashMap[K, V](32, 0.75f, true)

  override def get(blockId: K): V = {
    entries.synchronized {
      entries.get(blockId)
    }
  }

  override def put(blockId: K, block: V): V = {
    entries.synchronized {
      entries.put(blockId, block)
    }
  }

  override def remove(blockId: K): V = {
    entries.synchronized {
      entries.remove(blockId)
    }
  }

  override def clear(): Unit = {
    entries.synchronized {
      entries.clear()
    }
  }

  override def contains(blockId: K): Boolean = {
    entries.synchronized {
      entries.containsKey(blockId)
    }
  }

  override def entrySet(): util.Set[Entry[K, V]] = {
    entries.entrySet()
  }
}

private[storage] class FIFOMemoryManager[K, V] extends BaseMemoryManager[K, V]{
  private val entries = new util.LinkedHashMap[K, V](32, 0.75f, false)

  override def get(blockId: K): V = {
    entries.synchronized {
      entries.get(blockId)
    }
  }

  override def put(blockId: K, block: V): V = {
    entries.synchronized {
      entries.put(blockId, block)
    }
  }

  override def remove(blockId: K): V = {
    entries.synchronized {
      entries.remove(blockId)
    }
  }

  override def clear(): Unit = {
    entries.synchronized {
      entries.clear()
    }
  }

  override def contains(blockId: K): Boolean = {
    entries.synchronized {
      entries.containsKey(blockId)
    }
  }

  override def entrySet(): util.Set[Entry[K, V]] = {
    entries.entrySet()
  }
}
