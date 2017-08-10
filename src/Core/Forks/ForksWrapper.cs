using KVS.Forks.Core.Interfaces;
using System;
using System.Collections.Generic;

namespace KVS.Forks.Core
{
    public class ForksWrapper<TDataTypesEnum>
    {
        public ForksWrapper(IKeyValueStore<TDataTypesEnum> keyValueStore)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));

            if (!typeof(TDataTypesEnum).IsEnum)
                throw new ArgumentException($"{nameof(TDataTypesEnum)} must be an enumerated type");
        }

        private IKeyValueStore<TDataTypesEnum> _keyValueStore;
        public IKeyValueStore<TDataTypesEnum> KeyValueStore
        {
            get
            {
                return _keyValueStore;
            }
        }

        // Forks logic will be contained here, checking versions
        //
        // Use case:
        // var wrapper = new ForksWrapper<RedisDataTypesEnum>();
        // wrapper.Set<User>(type.String, "key", user, null);
        // wrapper.Set<UserInHash>(type.Hash, "key2", user, new RedisHashParams { HashKey = "hashKey" });

        public bool Set<T>(TDataTypesEnum type, string key, T value, object extraParams = null)
        {
            return KeyValueStore.Set(type, key, value, extraParams);
        }

        public bool Set<T>(TDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            return KeyValueStore.Set(type, values);
        }

        public T Get<T>(TDataTypesEnum type, string key, object extraParams = null)
        {
            return KeyValueStore.Get<T>(type, key,extraParams);
        }

        public IDictionary<string, T> Get<T>(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            return KeyValueStore.Get<T>(type, keys);
        }
    }
}
