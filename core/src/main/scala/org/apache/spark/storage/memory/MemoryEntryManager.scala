/*
 * Created by Marvin on 11/2/16.
 */

package org.apache.spark.storage.memory

import java.util
import java.util.Map.Entry
import java.util.{Comparator, TreeMap, HashMap}
import org.apache.spark.storage.BlockId

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

class FrequencyComparator[T] (frequency: util.HashMap[T,Long]) extends Comparator[T] {
  override def compare(k1: T, k2: T): Int = {
    frequency.get(k1) compare frequency.get(k2)
  }
}

private[storage] class LFUMemoryManager[K, V] extends BaseMemoryManager[K, V]{
  private val frequency = new HashMap[K, Long]()
  private val entries = new TreeMap[K, V](new FrequencyComparator[K](frequency))

  override def get(blockId: K): V = {
    entries synchronized {
      val block = entries remove blockId
      frequency synchronized {
        val freq = frequency get blockId
        frequency.put(blockId, freq+1)
      }
      entries.put(blockId, block)
    }
  }

  override def put(blockId: K, block: V): V = {
    frequency synchronized {
      frequency.put(blockId, 0)
      entries synchronized {
        entries.put(blockId, block)
      }
    }

  }

  override def remove(blockId: K): V = {
    entries synchronized {
      val block = entries remove blockId
      frequency synchronized {
        frequency remove blockId
      }
      block
    }
  }

  override def clear(): Unit = {
    entries synchronized {
      entries.clear()
      frequency synchronized {
        frequency.clear()
      }
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
