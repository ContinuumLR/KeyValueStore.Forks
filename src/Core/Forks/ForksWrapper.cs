﻿using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KVS.Forks.Core
{
    public class ForksWrapper<TDataTypesEnum>
    {
        private Fork Fork { get; set; }

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
            int appId, int forkId, ForkProvider<TDataTypesEnum> forkProvider)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
            _appId = appId;

            if (!typeof(TDataTypesEnum).IsEnum)
                throw new ArgumentException($"{nameof(TDataTypesEnum)} must be an enumerated type");

            _forkProvider = forkProvider;
            Fork = _forkProvider.GetFork(forkId);
            _forkProvider.ForkChanged += _forkProvider_ForkChanged;
        }

        private void _forkProvider_ForkChanged(object sender, ForkChangedEventArgs e)
        {
            if (e.ForkIds.Contains(Fork.Id))
                Fork = _forkProvider.GetFork(Fork.Id);
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

            var byteValue = value as byte[] ?? BinarySerializerHelper.SerializeObject(value);

            return KeyValueStore.Set(type, forkedKey, byteValue, extraParams);
        }

        public bool Set<T>(TDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values)
        {
            if (Fork.ReadOnly)
                return false;

            KeyValueStore.Delete(type, values.Select(x => Tuple.Create(KeyGenerator.GenerateForkNullKey(AppId, Fork.Id, x.Item1), x.Item3)));

            var forkedValues = values.Select(x => Tuple.Create(KeyGenerator.GenerateForkValueKey(AppId, Fork.Id, x.Item1), x.Item2 as byte[] ?? BinarySerializerHelper.SerializeObject(x.Item2), x.Item3));

            return KeyValueStore.Set(type, forkedValues);
        }

        public T Get<T>(TDataTypesEnum type, string key, object extraParams = null)
        {
            var currentFork = Fork;

            while (currentFork != null)
            {
                var byteValue = KeyValueStore.Get(type, KeyGenerator.GenerateForkValueKey(AppId, currentFork.Id, key), extraParams);

                if (byteValue != null)
                {
                    if (typeof(T) == typeof(byte[]))
                        return (T)Convert.ChangeType(byteValue, typeof(T));

                    return (T)BinarySerializerHelper.DeserializeObject(byteValue);
                }

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

                var byteValues = KeyValueStore.Get(type, keysForGet);

                if (typeof(T) == typeof(byte[]))
                {
                    foreach (var key in byteValues.Keys)
                        res[generatedKeyToOriginalKey[key]] = (T)Convert.ChangeType(byteValues[key], typeof(T));
                }
                else
                {
                    foreach (var key in byteValues.Keys)
                        res[generatedKeyToOriginalKey[key]] = (T)BinarySerializerHelper.DeserializeObject(byteValues[key]);
                }

                missingKeys = missingKeys.Except(byteValues.Keys.Select(x => generatedKeyToOriginalKey[x])).ToList();

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
            if (Fork.ReadOnly)
                return false;

            var res = KeyValueStore.Delete(type, KeyGenerator.GenerateForkValueKey(AppId, Fork.Id, key), extraParams);

            if (Fork.GetAllParents().Any(x => KeyValueStore.Exists(type, KeyGenerator.GenerateForkValueKey(AppId, x.Id, key), extraParams)))
                return KeyValueStore.Set(type, KeyGenerator.GenerateForkNullKey(AppId, Fork.Id, key), BinarySerializerHelper.SerializeObject(type), extraParams);

            return res;
        }

        public bool Delete(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys)
        {
            if (Fork.ReadOnly)
                return false;

            var success = true;

            foreach (var key in keys)
                success = success && Delete(type, key.Item1, key.Item2);

            return success;
        }
    }
}