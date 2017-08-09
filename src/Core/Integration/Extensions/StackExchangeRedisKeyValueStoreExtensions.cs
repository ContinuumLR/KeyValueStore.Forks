using KVS.Forks.Core.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KVS.Forks.Core.Impl.StackExchangeRedisKeyValueStore;

namespace KVS.Forks.Core.Extensions
{
    public static class StackExchangeRedisForksWrapperExtensions
    {
        public static bool StringSet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, T value)
        {
            return wrapper.Set(StackExchangeRedisDataTypesEnum.String, key, value);
        }

        public static T StringGet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key)
        {
            return wrapper.Get<T>(StackExchangeRedisDataTypesEnum.String, key);
        }
    }
}
