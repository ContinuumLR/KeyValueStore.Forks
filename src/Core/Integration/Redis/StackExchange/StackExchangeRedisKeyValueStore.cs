using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using KVS.Forks.Core.Redis.StackExchange.Params;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Redis.StackExchange
{
    public class StackExchangeRedisKeyValueStore : IKeyValueStore<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>
    {
        private ConnectionMultiplexer _redis;

        public StackExchangeRedisKeyValueStore(string redisConnectionString, int redisTimeout = 5000)
        {
            if (!string.IsNullOrEmpty(redisConnectionString))
            {
                ConnectToRedis(redisConnectionString, redisTimeout);
            }
        }

        private void ConnectToRedis(string redisConnectionString, int redisTimeout)
        {
            var redisOptions = new ConfigurationOptions
            {
                ResponseTimeout = redisTimeout,
                ConnectTimeout = redisTimeout,
                SyncTimeout = redisTimeout
            };
            redisOptions.EndPoints.Add(redisConnectionString);

            _redis = ConnectionMultiplexer.Connect(redisOptions);
        }

        public StackExchangeRedisDataTypesEnum DefaultType => StackExchangeRedisDataTypesEnum.String;

        public T Get<T>(StackExchangeRedisDataTypesEnum type, string key, object extraParams = null)
        {
            var db = _redis.GetDatabase();
            var value = new RedisValue();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    value = db.StringGet(key);
                    break;
                case StackExchangeRedisDataTypesEnum.Hash:
                    var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(extraParams);
                    value = db.HashGet(key, castedExtraParams.HashField);
                    break;
            }

            if (!value.IsNull)
                return (T)BinarySerializerHelper.DeserializeObject(value);
            else
                return default(T);
        }

        public IDictionary<string, T> Get<T>(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            var db = _redis.GetDatabase();

            var res = new Dictionary<string, T>();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    var stringValues = db.StringGet(keys.Select(x => (RedisKey)x.Item1).ToArray());

                    var keyList = keys.ToList();
                    for (int i = 0; i < keyList.Count; i++)
                    {
                        if (!stringValues[i].IsNull)
                            res[keyList[i].Item1] = (T)BinarySerializerHelper.DeserializeObject(stringValues[i]);
                    }

                    return res;
                case StackExchangeRedisDataTypesEnum.Hash:
                    var distincyKeys = keys.Select(x => x.Item1).Distinct();

                    if (distincyKeys.Count() > 1) throw new ArgumentException($"Using type {nameof(StackExchangeRedisDataTypesEnum.Hash)} - only one distinct key is allowed");
                    var key = distincyKeys.First();

                    var hashFields = keys.Select(x =>
                    {
                        var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(x.Item2);
                        return (RedisValue)castedExtraParams.HashField;
                    }).ToArray();

                    var hashValues = db.HashGet(key, hashFields);

                    for (int i = 0; i < keys.Count(); i++)
                    {
                        if (!hashValues[i].IsNull)
                            res[hashFields[i]] = (T)BinarySerializerHelper.DeserializeObject(hashValues[i]);
                    }
                    return res;
            }

            return null;
        }

        public bool Set<T>(StackExchangeRedisDataTypesEnum type, string key, T value, object extraParams = null)
        {
            var db = _redis.GetDatabase();

            var serializedValue = BinarySerializerHelper.SerializeObject(value);

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.StringSet(key, serializedValue);
                case StackExchangeRedisDataTypesEnum.Hash:
                    var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(extraParams);
                    return db.HashSet(key, castedExtraParams.HashField, serializedValue);
            }

            return false;
        }

        public bool Set<T>(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    var stringEntries = values.Select(x => new KeyValuePair<RedisKey, RedisValue>(x.Item1,
                        BinarySerializerHelper.SerializeObject(x.Item2))).ToArray();

                    return db.StringSet(stringEntries);
                case StackExchangeRedisDataTypesEnum.Hash:
                    var keys = values.Select(x => x.Item1).Distinct();

                    if (keys.Count() > 1) throw new ArgumentException($"Using type {nameof(StackExchangeRedisDataTypesEnum.Hash)} - only one distinct key is allowed");
                    var key = keys.First();

                    var hashEntries = values.Select(x =>
                    {
                        var serializedValue = BinarySerializerHelper.SerializeObject(x.Item2);
                        var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(x.Item3);
                        return new HashEntry(castedExtraParams.HashField, serializedValue);
                    }).ToArray();

                    db.HashSet(key, hashEntries);

                    return true;
            }

            return false;
        }

        public enum StackExchangeRedisDataTypesEnum
        {
            String,
            Hash
        }
    }
}
