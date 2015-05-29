using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace ZooKeeperClient.Extensions
{
    public static class ByteArrayExtensions
    {
        public static byte[] ToBytes(this Object obj)
        {
            if (obj == null)
                return null;

            var bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static T ToObject<T>(this byte[] bytes) where T : class
        {
            var memStream = new MemoryStream();
            var binForm = new BinaryFormatter();
            memStream.Write(bytes, 0, bytes.Length);
            memStream.Seek(0, SeekOrigin.Begin);
            var obj = binForm.Deserialize(memStream) as T;
            return obj;
        }
    }
}
