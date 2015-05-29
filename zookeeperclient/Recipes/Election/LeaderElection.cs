/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

using System;
using ZooKeeperNet;

namespace ZooKeeperClient.Recipes.Election
{
    public interface ILeaderWatcher
    {
        /// <summary>
        /// This is called when all of the below:
        /// <list type="bullet">
        /// <item>
        /// <description>After this process calls start</description>
        /// </item>
        /// <item>
        /// <description>The first time that we determine that we are leader</description>
        /// </item>
        /// </list>
        /// </summary>
        void TakeLeadership();
    }

    public class LeaderElection : ProtocolSupport
    {
        private readonly string _path;
        private string _id;
        private readonly byte[] _data;
        private ZNodeName _idName;
        private readonly ILeaderWatcher _watcher;
        public volatile bool IsOwner = false;

        public LeaderElection(ZooKeeper zookeeper, string path, ILeaderWatcher watcher, byte[] data)
            : base(zookeeper)
        {
            this._path = path;
            this._watcher = watcher;
            this._data = data;
        }

        public bool RunForLeader()
        {
            long sessionId = Zookeeper.SessionId;
            string prefix = "election-" + sessionId + "-";
            var names = Zookeeper.GetChildren(_path, false);
            // See whether we have already run for election in this process
            foreach (string name in names)
            {
                if (name.StartsWith(prefix))
                {
                    _id = name;
                    if (Log.IsDebugEnabled)
                    {
                        Log.DebugFormat("Found id created last time: {0}", _id);
                    }
                }
            }

            if (_id == null)
            {
                _id = Zookeeper.Create(_path.Combine(prefix), _data, Acl, CreateMode.EphemeralSequential);

                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Created id: {0}", _id);
                }
            }

            _idName = new ZNodeName(_id);

            names = Zookeeper.GetChildren(_path, false);
            var sortedNames = new SortedSet<ZNodeName>();
            foreach (var name in names)
            {
                sortedNames.Add(new ZNodeName(name));
            }

            var priors = sortedNames.HeadSet(_idName);

            if (priors.Count == 0)
            {
                throw new InvalidOperationException("Count of priors is 0, but should at least include this node.");
            }

            if (priors.Count == 1)
            {
                IsOwner = true;
                _watcher.TakeLeadership();
                return true;
            }
            // only watch the node directly before us
            ZNodeName penultimate = null, last = null;
            foreach (var name in sortedNames)
            {
                penultimate = last;
                last = name;
            }
            if (penultimate == null)
            {
                throw new InvalidOperationException("Penultimate value in priors is null, but count shoudl have been at least 2.");
            }
            var watchPath = _path.Combine(penultimate.Name);
            if (Zookeeper.Exists(watchPath, new LeaderWatcher(this, watchPath, _watcher)) == null)
            {
                IsOwner = true;
                _watcher.TakeLeadership();
                return true;
            }
            return false;
        }

        private class LeaderWatcher : IWatcher
        {
            private readonly LeaderElection election;
            private readonly string path;
            private readonly ILeaderWatcher watcher;

            public LeaderWatcher(LeaderElection election, string path, ILeaderWatcher watcher)
            {
                this.election = election;
                this.path = path;
                this.watcher = watcher;
            }

            public void Process(WatchedEvent @event)
            {
                if (@event.Type == EventType.NodeDeleted && @event.Path == path)
                {
                    election.IsOwner = true;
                    watcher.TakeLeadership();
                }
            }
        }

        public void Start()
        {
            EnsurePathExists(_path);

            RetryOperation(RunForLeader);
        }

        public void Close()
        {
            IsOwner = false;

            Zookeeper.Delete(_id, -1);
        }

        public override string ToString()
        {
            return string.Format("IdName: {0}, IsOwner: {1}", _idName, IsOwner);
        }
    }
}