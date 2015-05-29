using System;
using ZooKeeperNet;

namespace ZooKeeperClient
{
    public sealed class ZooKeeperFactory : IZooKeeperFactory
    {
        public static IZooKeeperFactory Instance = new ZooKeeperFactory();

        private ZooKeeperFactory() { }

        public ZooKeeper Connect(string address)
        {
            return Connect(address, new TimeSpan(0, 0, 0, 30), new Watcher());
        }

        public ZooKeeper Connect(string address, TimeSpan timeoutSpan)
        {
            return Connect(address, timeoutSpan, new Watcher());
        }

        public ZooKeeper Connect(string address, TimeSpan timeoutSpan, IWatcher watcher)
        {
            return new ZooKeeper(address, timeoutSpan, watcher);
        }

        public ZooKeeper Connect(string address, TimeSpan timeoutSpan, IWatcher watcher, long sessionId, byte[] password)
        {
            return new ZooKeeper(address, timeoutSpan, watcher, sessionId, password);
        }
    }
}
