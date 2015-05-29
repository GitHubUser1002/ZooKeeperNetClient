using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZooKeeperClient.Recipes.Queue;
using ZooKeeperNet;

namespace ZooKeeperClient.Test.Recipes
{
    [TestClass]
    public class QueueTest
    {
        private ZooKeeper _zk;
        private DistributedQueue _queue;
        private int _count = 2;

        [TestInitialize]
        public void Initialize()
        {
            var address = Configurations.ZooKeeperAddress;
            _zk = ZooKeeperFactory.Instance.Connect(address);
            _queue = new DistributedQueue(_zk, "/QueueTest");
        }

        [TestMethod]
        public void EnqueueDequeueTest()
        {
            var payload = new byte[] {1};
            _queue.Enqueue(payload);
            var result = _queue.Dequeue();

            Assert.IsTrue(payload.Count() == 1 && result.Count() == 1);

            Assert.IsTrue(payload[0] == result[0]);
        }
    }
}
