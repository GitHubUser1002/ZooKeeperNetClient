using System;

namespace ZooKeeperClient
{
    [Serializable]
    internal class DataTransferObject
    {
        public string Name { get; set; }

        public DataTransferObject(string name)
        {
            Name = name;
        }
    }
}
