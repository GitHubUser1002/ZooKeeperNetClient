using System;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace ZooKeeperClient
{
    internal class DataMonitor : IWatcher
    {
        private ZooKeeper _zk;

        private String _znode;

        private Watcher _chainedWatcher;

        public bool Dead { get; set; }

        private IDataMonitorListener _listener;

        private byte[] _prevData;

        public DataMonitor(ZooKeeper zk, String zPodePath, Watcher chainedWatcher,
            IDataMonitorListener listener)
        {
            _zk = zk;
            _znode = zPodePath;
            _chainedWatcher = chainedWatcher;
            _listener = listener;
            _zk.Exists(_znode, this);
        }

        public void Process(WatchedEvent @event)
        {
            var path = @event.Path;
            if (@event.Type == EventType.None)
            {
                // We are are being told that the state of the
                // connection has changed
                switch (@event.State)
                {
                    case KeeperState.SyncConnected:
                        // In this particular example we don't need to do anything
                        // here - watches are automatically re-registered with 
                        // server and any watches triggered while the client was 
                        // disconnected will be delivered (in order of course)
                        break;
                    case KeeperState.Expired:
                        // It's all over
                        Dead = true;
                        _listener.Closing(KeeperException.Code.SESSIONEXPIRED);
                        break;
                }
            }
            else
            {
                if (path != null && path.Equals(_znode))
                {
                    _zk.Exists(_znode, this);
                }
            }
            if (_chainedWatcher != null)
            {
                _chainedWatcher.Process(@event);
            }
        }

        public void ProcessResult(KeeperException.Code rc, String path, Object ctx, Stat stat)
        {
            bool exists;
            switch (rc)
            {
                case KeeperException.Code.OK:
                    exists = true;
                    break;
                case KeeperException.Code.NONODE:
                    exists = false;
                    break;
                case KeeperException.Code.SESSIONEXPIRED:
                case KeeperException.Code.NOAUTH:
                    Dead = true;
                    _listener.Closing(rc);
                    return;
                default:
                    // Retry errors
                    _zk.Exists(_znode, true);
                    return;
            }

            byte[] b = null;
            if (exists)
            {
                try
                {
                    b = _zk.GetData(_znode, false, null);
                }
                catch (KeeperException e)
                {
                    // We don't need to worry about recovering now. The watch
                    // callbacks will kick off any exception handling
                    //TODO
                }
                catch (Exception e)
                {
                    return;
                }
            }
            if ((b == null && b != _prevData)
                || (b != null && !Equals(_prevData, b)))
            {
                _listener.Exists(b);
                _prevData = b;
            }
        }
    }
}
