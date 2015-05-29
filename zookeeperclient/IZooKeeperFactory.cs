using System;
using ZooKeeperNet;

namespace ZooKeeperClient
{
    public interface IZooKeeperFactory
    {
        ZooKeeper Connect(string address);
        
        ZooKeeper Connect(string address, TimeSpan timeoutSpan);
        
        ZooKeeper Connect(string address, TimeSpan timeoutSpan, IWatcher watcher);

        ZooKeeper Connect(string address, TimeSpan timeoutSpan, IWatcher watcher, long sessionId, byte[] password);
    }
}
