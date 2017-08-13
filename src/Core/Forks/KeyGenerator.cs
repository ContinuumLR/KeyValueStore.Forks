using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core
{
    public class KeyGenerator
    {
        public static string GenerateForkValueKey(int appId, int forkId, string key)
        {
            return $"KVSF:{appId}:F:{forkId}:{key}";
        }
        public static string GenerateForkNullKey(int appId, int forkId, string key)
        {
            return $"KVSF:{GenerateForkValueKey(appId, forkId, key)}:KVSNull";
        }

        public static string GenerateForksKey(int appId)
        {
            return $"KVSF:{appId}:Forks";
        }
        
        public static string GenerateForkKey(int appId, int forkId)
        {
            return $"KVSF:{appId}:F:{forkId}";
        }
        public static string GenerateForkTimeStampKey(int appId, int forkId)
        {
            return $"KVSF:{appId}:F:{forkId}:TS";
        }

        public static string GenerateAppKey(int appId)
        {
            return $"KVSF:{appId}";
        }

        public static string AppsKey => "KVSF:Apps";
    }
}