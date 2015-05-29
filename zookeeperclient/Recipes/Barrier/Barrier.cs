using System;
using System.Linq;
using System.Threading;
using ZooKeeperNet;

namespace ZooKeeperClient.Recipes.Barrier
{
    public class Barrier
    {
        readonly String _root;
        private readonly int _size;
        private readonly String _nodeName;
        private readonly ZooKeeper _zk;

        /**
         * Barrier constructor
         *
         * @param address
         * @param root
         * @param size
         */
        public Barrier(ZooKeeper zk, String root, int size, string nodeName)
        {
            _root = root;
            _size = size;

            _nodeName = nodeName;

            _zk = zk;

            // Create barrier node
            if (_zk == null) return;

            try
            {
                var s = _zk.Exists(root, false);
                if (s == null)
                    _zk.Create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Keeper exception when instantiating queue: " + e);
            }
            catch (Exception e)
            {
                //TODO
            }
        }

        private readonly object _mutex = new object();

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        public bool Enter()
        {
            _zk.Create(_root + "/" + _nodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EphemeralSequential);

            while (true)
                lock(_mutex)
                {
                    var children = _zk.GetChildren(_root, true);

                    if (children.Count() < _size)
                        Monitor.Wait(_mutex);
                    else
                        return true;
                }
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        public bool Leave()
        {
            _zk.Delete(_root + "/" + _nodeName, 0);

            while (true)
                lock (_mutex)
                {
                    var children = _zk.GetChildren(_root, true);
                    if (children.Any())
                        Monitor.Wait(_mutex);
                    else
                        return true;
                }
        }

    }
}
