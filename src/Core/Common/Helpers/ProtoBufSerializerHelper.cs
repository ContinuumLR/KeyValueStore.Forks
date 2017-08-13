using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Helpers
{
    public static class ProtoBufSerializerHelper
    {
        public static byte[] Serialize<T>(T obj)
        {
            MemoryStream ms = new MemoryStream();
            Serializer.Serialize(ms, obj);

            return ms.ToArray();
        }

        public static T Deserialize<T>(byte[] arrBytes)
        {
            MemoryStream memStream = new MemoryStream();
            memStream.Write(arrBytes, 0, arrBytes.Length);
            memStream.Seek(0, SeekOrigin.Begin);
            return Serializer.Deserialize<T>(memStream);
        }
    }
}
