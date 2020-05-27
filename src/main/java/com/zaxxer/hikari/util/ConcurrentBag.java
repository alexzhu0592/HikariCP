/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * 这是一个专门用于连接池的并发包, 性能优于LinkedBlockingQueue和LinkedTransferQueue队列,
 * 通过使用ThreadLocal存储尽可能的避免锁, 只有ThreadLocal中没有可用的items
 * 才会扫描线程间共享的容器,
 * ThreadLocal中items没有在用的时候,可以被其他没有可用的items的线程窃取走.
 * 用特定同步器(AbstractQueuedLongSynchronizer)的无锁实现管理线程间信号.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * 这里要特别注意的是，ConcurrentBag中通过borrow方法进行数据资源借用，
 * 通过requite方法进行资源回收，注意其中borrow方法只提供对象引用，不移除对象。
 * 所以从bag中“借用”的items实际上并没有从任何集合中删除，因此即使引用废弃了，
 * 垃圾收集也不会发生。因此使用时通过borrow取出的对象必须通过requite方法进行
 * 放回，否则会导致内存泄露，只有"remove"方法才能完全从bag中删除一个对象。
 *
 * 理解sharedList和threadList在线程内和线程间的作用的时候, 应该放在更上一层
 * 即调用ConcurrentBag的那层{@link com.zaxxer.hikari.pool.HikariPool} ,
 * 在其中ConcurrentBag存放的是PoolEntry, 从字面意思可以理解为连接池入口,
 * 存放的是连接的相关信息, 比如Connection, 缓存的Statement, 是否只读,是否自动提交事务这些信息
 * 当在多线程环境下获取HikariPool的ConcurrentBag就会存在是否线程间和线程内的缓存问题
 *
 * 在SpringBoot集成Hikari的时候, 是怎么使用的呢?
 * springboot中hibernate会通过{@link com.zaxxer.hikari.hibernate.HikariConnectionProvider}
 * 这个提供进行获取和释放连接接口, 实现了hibernate的接口 {@link ConnectionProvider}
 * springboot中mybatis是通过实现{@linkplain UnpooledDataSourceFactory}接入hikariCP的,
 * 必须说下mybatis有实现连接池, 但是我们连接上去的时候, 是通MySQLDialect过非连接池的数据源接上去的
 *
 * hibernete也是自己连接池实现 , 可以通过 {@link org.hibernate.connection.DriverManagerConnectionProvider} 看到
 * 部分的实现:
 *         synchronized(this.pool) {
 *             if (!this.pool.isEmpty()) {
 *                 int last = this.pool.size() - 1;
 *                 if (log.isTraceEnabled()) {
 *                     log.trace("using pooled JDBC connection, pool size: " + last);
 *                     ++this.checkedOut;
 *                 }
 *
 *                 Connection pooled = (Connection)this.pool.remove(last);
 *                 if (this.isolation != null) {
 *                     pooled.setTransactionIsolation(this.isolation);
 *                 }
 *
 *                 if (pooled.getAutoCommit() != this.autocommit) {
 *                     pooled.setAutoCommit(this.autocommit);
 *                 }
 *
 *                 return pooled;
 *             }
 *         }
 *  这个pool就是连接池, 实际上是一个 private final ArrayList pool = new ArrayList(); 可见效率是非常低的
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

   /** 线程间的共享数据, 同时连接池是少写多读的场景, 用CopyOnWriteArrayList非常适合.
    * 常用的List有ArrayList、LinkedList、Vector，其中前两个是线程非安全的，最后一个
    * 是线程安全的。Vector虽然是线程安全的，但是只是一种相对的线程安全而不是绝对的线
    * 程安全，它只能够保证增、删、改、查的单个操作一定是原子的，不会被打断，但是如果
    * 组合起来用，并不能保证线程安全性。比如就像上面的线程1在遍历一个Vector中的元素、
    * 线程2在删除一个Vector中的元素一样，势必产生并发修改异常，也就是fail-fast。
    *
    * 缺点有二 :
    * 因为CopyOnWrite的写时复制机制，所以在进行写操作的时候，内存里会同时驻扎两个对象的内存
    * 占内存;
    * 能做到最终一致性,但是还是没法满足实时性要求；
    * **/
   private final CopyOnWriteArrayList<T> sharedList;
   private final boolean weakThreadLocals;

   /** 线程内的共享数据 **/
   private final ThreadLocal<List<Object>> threadList;
   private final IBagStateListener listener;
   private final AtomicInteger waiters;
   private volatile boolean closed;

   /** 主要用于存在资源等待线程时的第一手资源交接
    * SynchronousQueue是一个无存储空间的阻塞队列非常适合做交换工作，
    * 生产者的线程和消费者的线程同步以传递某些信息、事件或者任务。
    * 因为是无存储空间的，所以与其他阻塞队列实现不同的是，这个阻塞
    * peek方法直接返回null，无任何其他操作，其他的方法与阻塞队列的
    * 其他方法一致。这个队列的特点是，必须先调用take或者poll方法，
    * 才能使用off，add方法。 **/
   private final SynchronousQueue<T> handoffQueue;

   public interface IConcurrentBagEntry
   {
      int STATE_NOT_IN_USE = 0;
      int STATE_IN_USE = 1;
      int STATE_REMOVED = -1;
      int STATE_RESERVED = -2;

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   public interface IBagStateListener
   {
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      this.weakThreadLocals = useWeakThreadLocals();

      //公平模式总结下来就是：队尾匹配队头出队，先进先出，体现公平原则
      this.handoffQueue = new SynchronousQueue<>(true);
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    * 这个方法是线程间窃取items的核心代码, 具体思想可以参考类上的注释
    *
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first 尝试从线程内共享获取
      final List<Object> list = threadList.get();
      for (int i = list.size() - 1; i >= 0; i--) {
         final Object entry = list.remove(i);
         @SuppressWarnings("unchecked")
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }

      // Otherwise, scan the shared list ... then poll the handoff queue
      // 获取无果的话, 从线程间共享变量获取, 再无果就进行等待
      final int waiting = waiters.incrementAndGet();
      try {
         // 当无可用本地化资源时，遍历全部资源，查看是否存在可用资源
         // 因此被一个线程本地化的资源也可能被另一个线程“抢走”
         for (T bagEntry : sharedList) {
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  // 因为可能“抢走”了其他线程的资源，因此提醒包裹进行资源添加
                  listener.addBagItem(waiting - 1);
               }
               return bagEntry;
            }
         }

         listener.addBagItem(waiting);

         timeout = timeUnit.toNanos(timeout);
         do {
            // 当现有全部资源全部在使用中，等待一个被释放的资源或者一个新资源
            final long start = currentTime();
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            timeout -= elapsedNanos(start);
         } while (timeout > 10_000);

         return null;
      }
      finally {
         waiters.decrementAndGet();
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    * 返还借来的对象, 借出的对象一直不进行返还就会造成内存泄露
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry)
   {
      // 将状态转为未在使用
      bagEntry.setState(STATE_NOT_IN_USE);
      // 判断是否存在等待线程，若存在，则直接转手资源
      for (int i = 0; waiters.get() > 0; i++) {
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         else if ((i & 0xff) == 0xff) {
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            Thread.yield();
         }
      }
      // 否则，进行资源本地化
      final List<Object> threadLocalList = threadList.get();
      if (threadLocalList.size() < 50) {
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }
      //新添加的资源优先放入CopyOnWriteArrayList
      sharedList.add(bagEntry);

      // spin until a thread takes it or none are waiting
      // 当有等待资源的线程时，将资源交到某个等待线程后才返回（SynchronousQueue）
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
         Thread.yield();
      }
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry)
   {
      // 如果资源正在使用且无法进行状态切换，则返回失败
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      final boolean removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      threadList.get().remove(bagEntry);

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry)
   {
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   @SuppressWarnings("SpellCheckingInspection")
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      int count = 0;
      for (IConcurrentBagEntry e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final int[] states = new int[6];
      for (IConcurrentBagEntry e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
