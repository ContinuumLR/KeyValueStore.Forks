using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KVS.Forks.Core
{
    public class ForksWrapper<TDataTypesEnum>
    {
        private readonly Fork _fork;
        private Fork Fork
        {
            get
            {
                return _fork; //get fork from store
            }
        }

        private readonly int _appId;
        public int AppId
        {
            get
            {
                return _appId;
            }
        }

        public ForksWrapper(IKeyValueStore<TDataTypesEnum> keyValueStore,
            int appId, Fork fork)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
            _fork = fork ?? throw new ArgumentNullException(nameof(fork));
            _appId = appId;

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

        public bool Set<T>(TDataTypesEnum type, string key, T value, object extraParams = null)
        {
            if (Fork.ReadOnly)
                return false;

            var forkedKey = GenerateForkedKey(Fork, key);

            return KeyValueStore.Set(type, forkedKey, value, extraParams);
        }

        public bool Set<T>(TDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            if (Fork.ReadOnly)
                return false;

            var forkedValues = values.Select(x => Tuple.Create(GenerateForkedKey(Fork, x.Item1), x.Item2, x.Item3));

            return KeyValueStore.Set(type, forkedValues);
        }

        public T Get<T>(TDataTypesEnum type, string key, object extraParams = null)
        {
            var currentFork = Fork;

            while (currentFork != null)
            {
                var value = KeyValueStore.Get<T>(type, GenerateForkedKey(currentFork, key), extraParams);

                if (value != null)
                    return value;

                currentFork = currentFork.Parent;
            }

            return default(T);
        }

        public IDictionary<string, T> Get<T>(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            var currentFork = Fork;
            var missingKeys = keys.Select(x => x.Item1).ToList();

            // Easy access to tuple data
            var keysDict = keys.ToDictionary(x => x.Item1, x => x.Item2);

            var res = new Dictionary<string, T>();

            while (currentFork != null)
            {
                var keysForGet = new List<Tuple<string, object>>();
                var generatedKeyToOriginalKey = new Dictionary<string, string>();

                foreach (var key in missingKeys)
                {
                    var generatedKey = GenerateForkedKey(currentFork, key);
                    keysForGet.Add(Tuple.Create(generatedKey, keysDict[key]));
                    generatedKeyToOriginalKey[generatedKey] = key;
                }

                var values = KeyValueStore.Get<T>(type, keysForGet);
                
                foreach (var key in values.Keys)
                {
                    res[generatedKeyToOriginalKey[key]] = values[key];
                }

                missingKeys = missingKeys.Except(values.Keys.Select(x=> generatedKeyToOriginalKey[x])).ToList();

                if (missingKeys.Count == 0)
                    break;

                currentFork = currentFork.Parent;
            }

            return res;
        }

        private string GenerateForkedKey(Fork fork, string key)
        {
            return $"{AppId}:{fork.Id}:{key}";
        }
    }
}
