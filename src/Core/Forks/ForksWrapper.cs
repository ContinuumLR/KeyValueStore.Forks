using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KVS.Forks.Core
{
    public class ForksWrapper<TDataTypesEnum>
    {
        private Fork _fork;
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

        private readonly ForkProvider<TDataTypesEnum> _forkProvider;

        public ForksWrapper(IKeyValueStore<TDataTypesEnum> keyValueStore,
            int appId, Fork fork)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
            _fork = fork ?? throw new ArgumentNullException(nameof(fork));
            _appId = appId;

            _forkProvider = new ForkProvider<TDataTypesEnum>(keyValueStore, AppId, fork.Id);
            _forkProvider.ForkChanged += _forkProvider_ForkChanged;

            if (!typeof(TDataTypesEnum).IsEnum)
                throw new ArgumentException($"{nameof(TDataTypesEnum)} must be an enumerated type");
        }

        private void _forkProvider_ForkChanged(object sender, EventArgs e)
        {
            _fork = _forkProvider.CurrentFork;
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

            KeyValueStore.Delete(type, KeyGenerator.GenerateForkNullKey(AppId, Fork.Id, key), extraParams); ;

            var forkedKey = KeyGenerator.GenerateForkValueKey(AppId, Fork.Id, key);

            return KeyValueStore.Set(type, forkedKey, value, extraParams);
        }

        public bool Set<T>(TDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            if (Fork.ReadOnly)
                return false;

            KeyValueStore.Delete(type, values.Select(x => Tuple.Create(KeyGenerator.GenerateForkNullKey(AppId, Fork.Id, x.Item1), x.Item3)));

            var forkedValues = values.Select(x => Tuple.Create(KeyGenerator.GenerateForkValueKey(AppId, Fork.Id, x.Item1), x.Item2, x.Item3));

            return KeyValueStore.Set(type, forkedValues);
        }

        public T Get<T>(TDataTypesEnum type, string key, object extraParams = null)
        {
            var currentFork = Fork;

            while (currentFork != null)
            {
                var value = KeyValueStore.Get<T>(type, KeyGenerator.GenerateForkValueKey(AppId, currentFork.Id, key), extraParams);

                if (value != null)
                    return value;

                if (KeyValueStore.Exists(type, KeyGenerator.GenerateForkNullKey(AppId, currentFork.Id, key), null))
                    return default(T);

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
                    var generatedKey = KeyGenerator.GenerateForkValueKey(AppId, currentFork.Id, key);
                    keysForGet.Add(Tuple.Create(generatedKey, keysDict[key]));
                    generatedKeyToOriginalKey[generatedKey] = key;
                }

                var values = KeyValueStore.Get<T>(type, keysForGet);

                foreach (var key in values.Keys)
                {
                    res[generatedKeyToOriginalKey[key]] = values[key];
                }

                missingKeys = missingKeys.Except(values.Keys.Select(x => generatedKeyToOriginalKey[x])).ToList();

                var nullMissingKeys = new List<string>();
                foreach (var missingKey in missingKeys.ToList())
                {
                    if (KeyValueStore.Exists(type, KeyGenerator.GenerateForkNullKey(AppId, currentFork.Id, missingKey), keysDict[missingKey]))
                        nullMissingKeys.Add(missingKey);
                }

                missingKeys = missingKeys.Except(nullMissingKeys).ToList();

                if (missingKeys.Count == 0)
                    break;

                currentFork = currentFork.Parent;
            }

            return res;
        }

        public bool Delete(TDataTypesEnum type, string key, object extraParams = null)
        {
            var res = KeyValueStore.Delete(type, KeyGenerator.GenerateForkValueKey(AppId, Fork.Id, key), extraParams);

            if (Fork.GetAllParents().Any(x => KeyValueStore.Exists(type, KeyGenerator.GenerateForkValueKey(AppId, x.Id, key), extraParams)))
                return KeyValueStore.Set(type, KeyGenerator.GenerateForkNullKey(AppId, Fork.Id, key), 0, extraParams);

            return res;
        }

        public bool Delete(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            var success = true;

            foreach (var key in keys)
                success = success && Delete(type, key.Item1, key.Item2);

            return success;
        }
    }
}