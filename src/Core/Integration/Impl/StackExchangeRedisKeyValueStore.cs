using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Impl
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
            }

            if (!value.IsNull)
                return (T)BinarySerializerHelper.DeserializeObject(value);
            else
                return default(T);
        }

        public IEnumerable<T> Get<T>(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            throw new NotImplementedException();
        }

        public bool Set<T>(StackExchangeRedisDataTypesEnum type, string key, T value, object extraParams = null)
        {
            var db = _redis.GetDatabase();

            switch (type)
            {
                case StackExchangeRedisDataTypesEnum.String:
                    return db.StringSet(key, BinarySerializerHelper.SerializeObject(value));
            }

            return false;
        }

        public bool Set<T>(StackExchangeRedisDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            throw new NotImplementedException();
        }

        public enum StackExchangeRedisDataTypesEnum
        {
            String,
            Hash
        }
    }
}
