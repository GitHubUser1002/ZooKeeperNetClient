using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace ZooKeeperClient
{
    internal static class ZooKeperExtensions
    {
        public static void Connect(ref ZooKeeper zk, string address, string path)
        {
            if (zk == null)
                zk = new ZooKeeper(address, new TimeSpan(0, 0, 0, 30), new Watcher());
            
            var stats = zk.Exists(path, false);

            if (stats == null)
                zk.Create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
            
            var list = zk.GetChildren(path, true);
        }

        internal static bool Produce(this ZooKeeper zk, int i, string path)
        {
            var buffer = new Stack<byte>();
            buffer.Push(byte.Parse(i.ToString()));
            zk.Create(path + "/element", buffer.ToArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EphemeralSequential);
            return true;
        }

        private static string GetMinimumNode(IReadOnlyList<string> list)
        {
            var min = int.Parse(list[0].Substring(7));
            var minstr = list[0].Substring(7);

            foreach (var s in list)
            {
                var tempValue = int.Parse(s.Substring(7));
                if (tempValue >= min) continue;
                min = tempValue;
                minstr = list[0].Substring(7);
            }

            return minstr;
        }

        private static int Dequeue(IZooKeeper zk, string path, IReadOnlyList<string> nodes)
        {
            Stat stat = null;

            //Get minimum node
            var minstr = GetMinimumNode(nodes);

            var b = zk.GetData(path + "/element" + minstr, false, stat);

            zk.Delete(path + "/element" + minstr, 0);

            var buffer = Convert.ToInt32(b.First());
            var retvalue = buffer;
            return retvalue;
        }

        internal static int Consume(this ZooKeeper zk, string path)
        {
            while (true)
            {
                //Get child nodes
                var nodes = zk.GetChildren(path, true).ToList();
                if (nodes.IsEmpty()) continue;

                //Dequeue minimum node
                return Dequeue(zk, path, nodes);
            }
        }
    }
}
