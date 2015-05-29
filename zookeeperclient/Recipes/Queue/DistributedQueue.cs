using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using log4net;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace ZooKeeperClient.Recipes.Queue
{
    public class DistributedQueue
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DistributedQueue));

        private readonly string _dir;
        private readonly ZooKeeper _zookeeper;
        private readonly List<ACL> _acl = Ids.OPEN_ACL_UNSAFE;

        private const string Prefix = "qn-";

        public DistributedQueue(ZooKeeper zookeeper, string dir)
        {
            this._dir = dir;
            this._zookeeper = zookeeper;
        }

        public DistributedQueue(ZooKeeper zookeeper, string dir, List<ACL> acl)
        {
            this._zookeeper = zookeeper;
            this._dir = dir;
            if (acl != null) this._acl = acl;
        }

        private SortedDictionary<long, string> OrderedChildren(IWatcher watcher)
        {
            var orderedChildren = new SortedDictionary<long, string>();

            foreach (string childName in _zookeeper.GetChildren(_dir, watcher))
            {
                try
                {
                    bool matches = childName.Length > Prefix.Length && childName.Substring(0, Prefix.Length) == Prefix;
                    if (!matches)
                    {
                        Log.WarnFormat("Found child node with improper name: {0}", childName);
                        continue;
                    }
                    string suffix = childName.Substring(Prefix.Length);
                    long childId = Convert.ToInt64(suffix);
                    orderedChildren[childId] = childName;
                }
                catch (InvalidCastException e)
                {
                    Log.WarnFormat("Found child node with improper format : {0} {1} {2}", childName, e, e.StackTrace);
                }
            }

            return orderedChildren;
        }

        public byte[] Peek()
        {
            try
            {
                return GetElement(false);
            }
            catch (NoSuchElementException)
            {
                return null;
            }
        }

        /// <summary>
        /// Adds an item to the queue
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        public bool Enqueue(byte[] data)
        {
            for (; ; )
            {
                try
                {
                    _zookeeper.Create(_dir + "/" + Prefix, data, _acl, CreateMode.PersistentSequential);
                    return true;
                }
                catch (KeeperException.NoNodeException)
                {
                    _zookeeper.Create(_dir, new byte[0], _acl, CreateMode.Persistent);
                }
            }
        }

        /// <summary>
        /// Removes an item from the queue.  If an item is not available, a <see cref="NoSuchElementException">NoSuchElementException</see>
        /// is thrown.
        /// </summary>
        /// <returns></returns>
        public byte[] Dequeue()
        {
            return GetElement(true);
        }

        /// <summary>
        /// Removes an item from the queue.  If an item is not available, then the method blocks until one is.
        /// </summary>
        /// <returns></returns>
        public byte[] Take()
        {
            byte[] data;
            TryTakeInternal(Int32.MaxValue, out data);
            return data;
        }

        public bool TryTake(TimeSpan timeout, out byte[] data)
        {
            var time = timeout == TimeSpan.MaxValue ? Int32.MaxValue : Convert.ToInt32(timeout.TotalMilliseconds);
            return TryTakeInternal(time, out data);
        }

        private bool TryTakeInternal(int wait, out byte[] data)
        {
            data = null;
            SortedDictionary<long, string> orderedChildren;
            while (true)
            {
                ResetChildWatcher childWatcher = new ResetChildWatcher();
                try
                {
                    orderedChildren = OrderedChildren(childWatcher);
                }
                catch (KeeperException.NoNodeException)
                {
                    _zookeeper.Create(_dir, new byte[0], _acl, CreateMode.Persistent);
                    continue;
                }
                if (orderedChildren.Count == 0)
                {
                    if (!childWatcher.WaitOne(wait)) return false;
                    continue;
                }

                foreach (string path in orderedChildren.Values.Select(headNode => _dir.Combine(headNode)))
                {
                    try
                    {
                        data = _zookeeper.GetData(path, false, null);
                        _zookeeper.Delete(path, -1);
                        return true;
                    }
                    catch (KeeperException.NoNodeException)
                    {
                        // Another client deleted the node first.
                    }
                }
            }
        }

        private byte[] GetElement(bool delete)
        {
            SortedDictionary<long, string> orderedChildren;

            while (true)
            {
                try
                {
                    orderedChildren = OrderedChildren(null);
                }
                catch (KeeperException.NoNodeException)
                {
                    throw new NoSuchElementException();
                }

                foreach (string path in orderedChildren.Values.Select(head => _dir.Combine(head)))
                {
                    try
                    {
                        byte[] data = _zookeeper.GetData(path, false, null);
                        if (delete) _zookeeper.Delete(path, -1);
                        return data;
                    }
                    catch (KeeperException.NoNodeException)
                    {
                    }
                }
            }
        }

        private class ResetChildWatcher : IWatcher
        {
            private readonly ManualResetEvent reset;

            public ResetChildWatcher()
            {
                reset = new ManualResetEvent(false);
            }

            public void Process(WatchedEvent @event)
            {
                Log.DebugFormat("Watcher fired on path: {0} state: {1} type {2}", @event.Path, @event.State, @event.Type);
                reset.Set();
            }

            public bool WaitOne(int wait)
            {
                return reset.WaitOne(wait);
            }
        }
    }

    public class NoSuchElementException : Exception
    {
    }
}