﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using log4net;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace ZooKeeperClient.Recipes.Election
{
    public abstract class ProtocolSupport : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(ProtocolSupport));

        private int _closed;

        public ProtocolSupport(IZooKeeper zookeeper)
        {
            RetryDelay = new TimeSpan(0, 0, 0, 0, 500);
            Acl = Ids.OPEN_ACL_UNSAFE;
            RetryCount = 10;
            Zookeeper = zookeeper;
        }

        public IZooKeeper Zookeeper { get; set; }

        public TimeSpan RetryDelay { get; set; }

        public List<ACL> Acl { get; set; }

        public int RetryCount { get; set; }


        /**
         * Perform the given operation, retrying if the connection fails
         * @return object. it needs to be cast to the callee's expected 
         * return type.
         */
        protected T RetryOperation<T>(Func<T> operation)
        {
            KeeperException exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    return operation();
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    Log.WarnFormat("Session expired for: {0} so reconnecting due to: {1} {2}", Zookeeper, e, e.StackTrace);
                    throw e;
                }
                catch (KeeperException.ConnectionLossException e)
                {
                    if (exception == null)
                        exception = e;
                    Log.DebugFormat("Attempt {0} failed with connection loss so attempting to reconnect: {1} {2}", e, e.StackTrace);
                    DoRetryDelay(i);
                }
                catch (TimeoutException e)
                {
                    if (exception == null)
                        exception = KeeperException.Create(KeeperException.Code.OPERATIONTIMEOUT);
                    Log.DebugFormat("Attempt {0} failed with connection loss so attempting to reconnect: {1} {2}", e, e.StackTrace);
                    DoRetryDelay(i);
                }
            }
            throw exception;
        }

        /**
         * Ensures that the given path exists with no data, the current
         * ACL and no flags
         * @param path
         */
        protected void EnsurePathExists(string path)
        {
            EnsureExists(path, null, Acl, CreateMode.Persistent);
        }

        /**
         * Ensures that the given path exists with the given data, ACL and flags
         * @param path
         * @param acl
         * @param flags
         */
        protected void EnsureExists(string path, byte[] data, List<ACL> acl, CreateMode flags)
        {
            try
            {
                var components = path.Split(PathUtils.PathSeparatorCharAsArray, StringSplitOptions.RemoveEmptyEntries);
                var pathBuilder = new StringBuilder(path.Length);

                for (var i = 0; i < components.Length; ++i)
                {
                    // Create a loop-scoped variable to avoid closure troubles
                    // inside RetryOperation.
                    var isLastComponent = (i == components.Length - 1);

                    var component = components[i];

                    pathBuilder.Append(PathUtils.PathSeparator);
                    pathBuilder.Append(component);
                    var currentPartialPath = pathBuilder.ToString();

                    RetryOperation(() =>
                    {
                        var stat = Zookeeper.Exists(currentPartialPath, false);
                        if (stat != null)
                        {
                            return true;
                        }
                        Zookeeper.Create(currentPartialPath, isLastComponent ? data : null, acl, flags);
                        return true;
                    });
                }
            }
            catch (KeeperException e)
            {
                Log.WarnFormat("Caught: {0} {1}", e, e.StackTrace);
            }
            catch (ThreadInterruptedException e)
            {
                Log.WarnFormat("Caught: {0} {1}", e, e.StackTrace);
            }
        }

        /**
         * Returns true if this protocol has been closed
         * @return true if this protocol is closed
         */
        protected bool IsDisposed()
        {
            return Interlocked.CompareExchange(ref _closed, 0, 0) == 1;
        }

        /**
         * Performs a retry delay if this is not the first attempt
         * @param attemptCount the number of the attempts performed so far
         */
        protected void DoRetryDelay(int attemptCount)
        {
            if (attemptCount > 0)
            {
                try
                {
                    Thread.Sleep(Convert.ToInt32(attemptCount * RetryDelay.TotalMilliseconds));
                }
                catch (ThreadInterruptedException e)
                {
                    Log.WarnFormat("Failed to sleep: {0} {1}", e, e.StackTrace);
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _closed, 1, 0) == 0)
            {
                GC.SuppressFinalize(this);
            }
        }

        ~ProtocolSupport()
        {
            Interlocked.Exchange(ref _closed, 1);
        }

        #endregion
    }
}