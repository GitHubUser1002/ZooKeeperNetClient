using ZooKeeperNet;

namespace ZooKeeperClient
{
    internal interface IDataMonitorListener
    {
        void Exists(byte[] data);

        void Closing(KeeperException.Code rc);
    }
}
