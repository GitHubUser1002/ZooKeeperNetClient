using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZooKeeperClient.Recipes.Barrier;
using ZooKeeperNet;

namespace ZooKeeperClient.Test.Recipes
{
    [TestClass]
    public class BarrierTest
    {
        private ZooKeeper _zk;
        private Barrier _barrier;
        private const int _count = 2;

        [TestInitialize]
        public void Initialize()
        {
            var address = Configurations.ZooKeeperAddress;
            _zk = ZooKeeperFactory.Instance.Connect(address);
            _barrier = new Barrier(_zk, "/BarrierTest", _count, "Node");
        }

        [TestMethod]
        public void BarrierEnterTest()
        {
            var tasks = Enumerable
                .Range(1, 2)
                .Select(i => Task.Factory.StartNew(() => { _barrier.Enter(); }))
                .ToArray();

            var completed = Task.WaitAll(tasks, 10*1000);

            Assert.IsTrue(completed);
        }
    }
}
