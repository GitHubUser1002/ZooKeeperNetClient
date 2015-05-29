using System;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZooKeeperClient.Recipes.Election;
using ZooKeeperNet;

namespace ZooKeeperClient.Test.Recipes
{
    [TestClass]
    public class ElectionTest
    {
        private ZooKeeper _zk;
        private LeaderElection _election;
        private LeaderWatcher _watcher;

        [TestInitialize]
        public void Initialize()
        {
            var address = Configurations.ZooKeeperAddress;
            _zk = ZooKeeperFactory.Instance.Connect(address);
            _election = new LeaderElection(_zk, "/ElectionTest", (_watcher = new LeaderWatcher()), new byte[0]);
        }

        private class LeaderWatcher : ILeaderWatcher
        {
            public event Action<object, EventArgs> LeadershipTaken;

            private void OnLeadershipTaken(object sender, EventArgs args)
            {
                var del = LeadershipTaken;
                if (del != null)
                    del(sender, args);
            }

            public void TakeLeadership()
            {
                OnLeadershipTaken(this, new EventArgs());     
            }
        }

        [TestMethod]
        public void StartElectionTest()
        {
            var @event = new AutoResetEvent(false);
            var del = new Action<object, EventArgs>((sender, args) => @event.Set());

            _watcher.LeadershipTaken += del;

            _election.Start();

            var completed = @event.WaitOne(10 * 1000);

            Assert.IsTrue(completed);
        }
    }
}
