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

        public byte[] Get(StackExchangeRedisDataTypesEnum type, string key, object extraParams = null)
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
                return value;
            else
                return null;
        }

        public IDictionary<string, byte[]> Get(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            var db = _redis.GetDatabase();

            var res = new Dictionary<string, byte[]>();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    var stringValues = db.StringGet(keys.Select(x => (RedisKey)x.Item1).ToArray());

                    var keyList = keys.ToList();
                    for (int i = 0; i < keyList.Count; i++)
                    {
                        if (!stringValues[i].IsNull)
                            res[keyList[i].Item1] = stringValues[i];
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
                            res[hashFields[i]] = hashValues[i];
                    }
                    return res;
            }

            return null;
        }

        public bool Set(StackExchangeRedisDataTypesEnum type, string key, byte[] value, object extraParams = null)
        {
            var db = _redis.GetDatabase();
            
            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.StringSet(key, value);
                case StackExchangeRedisDataTypesEnum.Hash:
                    var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(extraParams);
                    return db.HashSet(key, castedExtraParams.HashField, value);
            }

            return false;
        }

        public bool Set(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, byte[], object>> values)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    var stringEntries = values.Select(x => new KeyValuePair<RedisKey, RedisValue>(x.Item1,
                        x.Item2)).ToArray();

                    return db.StringSet(stringEntries);
                case StackExchangeRedisDataTypesEnum.Hash:
                    var keys = values.Select(x => x.Item1).Distinct();

                    if (keys.Count() > 1) throw new ArgumentException($"Using type {nameof(StackExchangeRedisDataTypesEnum.Hash)} - only one distinct key is allowed");
                    var key = keys.First();

                    var hashEntries = values.Select(x =>
                    {
                        var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(x.Item3);
                        return new HashEntry(castedExtraParams.HashField, x.Item2);
                    }).ToArray();

                    db.HashSet(key, hashEntries);

                    return true;
            }

            return false;
        }

        public bool Delete(StackExchangeRedisDataTypesEnum type, string key, object extraParams)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.KeyDelete(key);
                case StackExchangeRedisDataTypesEnum.Hash:
                    if (extraParams == null)
                        return db.KeyDelete(key);
                    else
                    {
                        var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(extraParams);
                        if (!string.IsNullOrEmpty(castedExtraParams.HashField))
                            return db.HashDelete(key, castedExtraParams.HashField);
                        else
                            return db.KeyDelete(key);
                    }
            }

            return false;
        }

        public bool Delete(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.KeyDelete(keys.Select(x => (RedisKey)x.Item1).ToArray()) == keys.Count();
                case StackExchangeRedisDataTypesEnum.Hash:
                    if (keys.Select(x => x.Item1).Distinct().Count() > 1) throw new ArgumentException($"Using type {nameof(StackExchangeRedisDataTypesEnum.Hash)} - only one distinct key is allowed");
                    var key = keys.First().Item1;

                    var hashFields = keys.Select(x =>
                    {
                        var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(x.Item2);
                        return (RedisValue)castedExtraParams.HashField;
                    }).ToArray();

                    return db.HashDelete(key, hashFields) == keys.Count();
            }

            return false;
        }

        public bool Exists(StackExchangeRedisDataTypesEnum type, string key, object extraParams)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.KeyExists(key);
                case StackExchangeRedisDataTypesEnum.Hash:
                    var castedExtraParams = ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(extraParams);
                    return db.HashExists(key, castedExtraParams.HashField);
            }

            return false;
        }

        /// <summary>
        /// Delete all keys in the store matching the pattern
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public bool FlushKeys(string pattern)
        {
            try
            {
                var db = _redis.GetDatabase();

                //Delete all keys matching the pattern
                var deletionScript = $"local keys = redis.call('keys', '{pattern}') \n for i = 1,#keys,5000 do \n redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) \n end \n return keys";
                var preparedScript = LuaScript.Prepare(deletionScript);
                db.ScriptEvaluate(preparedScript);
            }
            catch
            {
                return false;
            }

            return true;
        }

        public string[] Keys(string pattern)
        {
            var redisServer = _redis.GetServer(_redis.GetEndPoints().First());
            return redisServer.Keys(0, pattern).Select(x => (string)x).ToArray();
        }

        public IEnumerable<Tuple<StackExchangeRedisDataTypesEnum, byte[], object>> GetKeyData(string key)
        {
            var db = _redis.GetDatabase();

            var redisType = db.KeyType(key);
            if (redisType == RedisType.None)
                throw new Exception($"Key {key} returned type None");

            var type = ConvertRedisType(redisType);
            var res = new List<Tuple<StackExchangeRedisDataTypesEnum, byte[], object>>();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    var value = db.StringGet(key);
                    res.Add(Tuple.Create<StackExchangeRedisDataTypesEnum, byte[], object>(type, value, null));
                    break;
                case StackExchangeRedisDataTypesEnum.Hash:
                    var fields = db.HashGetAll(key);

                    foreach (var field in fields)
                    {
                        res.Add(Tuple.Create<StackExchangeRedisDataTypesEnum, byte[], object>(type, field.Value, new StackExchangeRedisHashParams
                        {
                            HashField = field.Name
                        }));
                    }
                    break;
                default:
                    break;
            }

            return res;
        }

        private StackExchangeRedisDataTypesEnum ConvertRedisType(RedisType redisType)
        {
            switch (redisType)
            {
                case RedisType.String:
                    return StackExchangeRedisDataTypesEnum.String;
                case RedisType.Hash:
                    return StackExchangeRedisDataTypesEnum.Hash;
                default:
                    return StackExchangeRedisDataTypesEnum.String;
            }
        }
        
        public enum StackExchangeRedisDataTypesEnum
        {
            String,
            Hash
        }
    }
}
