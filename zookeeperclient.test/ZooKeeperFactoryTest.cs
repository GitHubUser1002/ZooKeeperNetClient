using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZooKeeperNet;

namespace ZooKeeperClient.Test
{
    [TestClass]
    public class ZooKeeperFactoryTest
    {
        private string _address;
        private IZooKeeperFactory _factory;

        [TestInitialize]
        public void Initialize()
        {
            _address = "127.0.0.1:2181";
            _factory = ZooKeeperFactory.Instance;
        }

        [TestMethod]
        public void BasicConnectionTest()
        {
            var zk = _factory.Connect(_address);

            zk.Create("/BasicConnectionTest", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);

            var state = zk.State;

            Assert.AreEqual(state, ZooKeeper.States.CONNECTED);
        }
    }
}
