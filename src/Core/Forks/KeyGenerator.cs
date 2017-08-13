using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core
{
    public class KeyGenerator
    {
        public static string GenerateForkedKey(int appId, int forkId, string key)
        {
            return $"{appId}:{forkId}:{key}";
        }
        public static string GenerateNullKey(int appId, int forkId, string key)
        {
            return $"{GenerateForkedKey(appId, forkId, key)}:KVSNull";
        }

        public static string GenerateForkKey(int appId, int forkId)
        {
            return $"{appId}:F:{forkId}";
        }
        public static string GenerateForkTimeStampKey(int appId, int forkId)
        {
            return $"{appId}:FTS:{forkId}";
        }
    }
}