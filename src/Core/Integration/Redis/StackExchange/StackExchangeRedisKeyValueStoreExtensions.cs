using KVS.Forks.Core.Redis.StackExchange.Params;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KVS.Forks.Core.Redis.StackExchange.StackExchangeRedisKeyValueStore;

namespace KVS.Forks.Core.Redis.StackExchange
{
    public static class StackExchangeRedisForksWrapperExtensions
    {
        public static bool StringSet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, T value)
        {
            return wrapper.Set(StackExchangeRedisDataTypesEnum.String, key, value);
        }

        public static bool StringSet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, KeyValuePair<string, T>[] values)
        {
            return wrapper.Set(StackExchangeRedisDataTypesEnum.String, values.Select(x => Tuple.Create<string, T, object>(x.Key, x.Value, null)));
        }

        public static T StringGet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key)
        {
            return wrapper.Get<T>(StackExchangeRedisDataTypesEnum.String, key);
        }

        public static IDictionary<string, T> StringGet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string[] keys)
        {
            return wrapper.Get<T>(StackExchangeRedisDataTypesEnum.String, keys.Select(x => Tuple.Create<string, object>(x, null)));
        }

        public static bool HashSet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, string hashField, T value)
        {
            return wrapper.Set(StackExchangeRedisDataTypesEnum.Hash, key, value, new StackExchangeRedisHashParams { HashField = hashField });
        }

        public static bool HashSet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, KeyValuePair<string, T>[] hashFields)
        {
            return wrapper.Set(StackExchangeRedisDataTypesEnum.Hash, hashFields.Select(x => Tuple.Create<string, T, object>(key, x.Value, new StackExchangeRedisHashParams { HashField = x.Key })));
        }

        public static T HashGet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, string hashField)
        {
            return wrapper.Get<T>(StackExchangeRedisDataTypesEnum.Hash, key, new StackExchangeRedisHashParams { HashField = hashField });
        }

        public static IDictionary<string, T> HashGet<T>(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, string[] hashFields)
        {
            return wrapper.Get<T>(StackExchangeRedisDataTypesEnum.Hash, hashFields.Select(x => Tuple.Create<string, object>(key, new StackExchangeRedisHashParams { HashField = x })));
        }

        public static bool KeyDelete(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key)
        {
            return wrapper.Delete(StackExchangeRedisDataTypesEnum.String, key);
        }

        public static bool KeyDelete(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string[] keys)
        {
            return wrapper.Delete(StackExchangeRedisDataTypesEnum.String, keys.Select(x => Tuple.Create<string, object>(x, null)).ToArray());
        }

        public static bool HashDelete(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, string hashField)
        {
            return wrapper.Delete(StackExchangeRedisDataTypesEnum.Hash, key, new StackExchangeRedisHashParams { HashField = hashField });
        }

        public static bool HashDelete(this ForksWrapper<StackExchangeRedisDataTypesEnum> wrapper, string key, string[] hashFields)
        {
            return wrapper.Delete(StackExchangeRedisDataTypesEnum.Hash,
                hashFields.Select(x => Tuple.Create<string, object>(key,
                new StackExchangeRedisHashParams { HashField = x })).ToArray());
        }
    }
}
