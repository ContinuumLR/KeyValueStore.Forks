using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KVS.Forks.Core
{
    /// <summary>
    /// Manage the apps and forks
    /// </summary>
    /// <typeparam name="TDataTypesEnum"></typeparam>
    public class ForksManager<TDataTypesEnum>
    {
        private IKeyValueStore<TDataTypesEnum> _keyValueStore;
        public IKeyValueStore<TDataTypesEnum> KeyValueStore
        {
            get
            {
                return _keyValueStore;
            }
        }

        private int _appId;
        public int AppId
        {
            get
            {
                return _appId;
            }
        }

        public ForksManager(IKeyValueStore<TDataTypesEnum> keyValueStore)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
        }

        public ForksManager(IKeyValueStore<TDataTypesEnum> keyValueStore, int appId)
        {
            _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));

            SetApp(appId);
        }

        /// <summary>
        /// Sets the app Id
        /// </summary>
        /// <param name="appId"></param>
        public void SetApp(int appId)
        {
            if (appId == 0)
                throw new ArgumentException(nameof(appId));

            //Check if app exists

            _appId = appId;
        }

        /// <summary>
        /// Creates a new app and a master fork
        /// On success sets the app id
        /// </summary>
        /// <param name="appId"></param>
        /// <param name="name"></param>
        /// <param name="description"></param>
        public void CreateApp(int appId, string name, string description)
        {
            var bytesAppIds = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.AppsKey, null);
            List<int> appIds = null;

            if (bytesAppIds != null)
            {
                appIds = (List<int>)BinarySerializerHelper.DeserializeObject(bytesAppIds);
            }
            else
            {
                appIds = new List<int>();
                KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.AppsKey, BinarySerializerHelper.SerializeObject(appIds), null);
            }

            if (appIds.Contains(appId))
                throw new ArgumentException(nameof(appId));

            appIds.Add(appId);

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.AppsKey, BinarySerializerHelper.SerializeObject(appIds), null);

            var res = new App
            {
                Id = appId,
                Name = name,
                Description = description
            };

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateAppKey(appId), ProtoBufSerializerHelper.Serialize(res), null);

            SetApp(appId);

            CreateMasterFork();
        }

        private void CreateMasterFork()
        {
            var forkIds = new List<int>();
            forkIds.Add(1);

            var masterFork = new Fork
            {
                Id = 1,
                Name = "master"
            };

            SetForkIds(forkIds);
            SetFork(masterFork);
        }

        public void CreateFork(int id, string name, string description, int parentForkId)
        {
            var parentFork = GetFork(parentForkId);

            var forkIds = GetForkIds();
            
            if (forkIds.Contains(id))
                throw new ArgumentException(nameof(id));

            forkIds.Add(id);
            SetForkIds(forkIds);

            var newFork = new Fork
            {
                Id = id,
                Name = name,
                Description = description,
                Parent = parentFork
            };
            parentFork.Children.Add(newFork);
            SetFork(newFork);
            SetFork(parentFork);
        }
        
        public bool DeleteFork(int id)
        {
            var forkIds = GetForkIds();

            if (!forkIds.Contains(id))
                throw new ArgumentException(nameof(id));

            var fork = GetFork(id) ;

            if (fork.ReadOnly)
                return false;
            
            if (fork.Parent != null)
            {
                fork.Parent.Children.Remove(fork);
                SetFork(fork.Parent);
            }

            forkIds.Remove(fork.Id);
            SetForkIds(forkIds);

            KeyValueStore.FlushKeys($"{KeyGenerator.GenerateForkPattern(AppId, id)}*");

            return true;
        }

        /// <summary>
        /// Merge all the keys from one fork to another fork, a new fork is created from the target with the new data
        /// </summary>
        /// <param name="originForkId">Origin fork id</param>
        /// <param name="targetForkId">Target fork id</param>
        /// <returns>New fork id, Target fork id is the parent</returns>
        public int MergeFork(int originForkId, int targetForkId)
        {
            var usedKeys = new HashSet<string>();
            var valuesToSet = new List<Tuple<string, TDataTypesEnum, byte[], object>>();
            var keysToDelete = new Dictionary<string, TDataTypesEnum>();

            var currentFork = GetFork(originForkId);
            var targetFork = GetFork(targetForkId);

            while(!IsCommonParent(currentFork, targetFork))
            {
                var forkPattern = KeyGenerator.GenerateForkValuePattern(AppId, currentFork.Id);
                var keys = KeyValueStore.Keys($"{forkPattern}*");

                foreach (var key in keys)
                {
                    var originalKey = key.Substring(forkPattern.Length);

                    // If the key was used/deleted in a lower fork, this key is not relevant
                    if (usedKeys.Contains(originalKey) || keysToDelete.ContainsKey(originalKey))
                        continue;

                    if (originalKey.EndsWith(KeyGenerator.NullKeyPostFix))
                    {
                        var type = (TDataTypesEnum)BinarySerializerHelper.DeserializeObject(KeyValueStore.Get(KeyValueStore.DefaultType, key, null));
                        keysToDelete.Add(originalKey.Substring(0,originalKey.Length - KeyGenerator.NullKeyPostFix.Length), type);
                    }
                    else
                    {
                        var keyDataCollection = KeyValueStore.GetKeyData(key);

                        foreach (var keyData in keyDataCollection)
                        {
                            valuesToSet.Add(Tuple.Create(originalKey, keyData.Item1, keyData.Item2, keyData.Item3));
                        }

                        usedKeys.Add(originalKey);
                    }
                }

                currentFork = currentFork.Parent;
            }

            CreateFork(100, $"{targetFork.Name} Merge", "", targetFork.Id);

            var wrapper = GetWrapper(100);

            foreach (var keyToDelete in keysToDelete)
                wrapper.Delete(keyToDelete.Value, keyToDelete.Key);

            foreach (var value in valuesToSet)
            {
                wrapper.Set(value.Item2, value.Item1, value.Item3, value.Item4);
            }

            return 100;
        }

        private bool IsCommonParent(Fork fork, Fork targetFork)
        {
            var currentFork = targetFork;

            while(currentFork != null)
            {
                if (currentFork.Id == fork.Id)
                    return true;

                currentFork = currentFork.Parent;
            }
            
            return false;
        }

        private List<int> GetForkIds()
        {
            return (List<int>)BinarySerializerHelper.DeserializeObject(KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), null));
        }

        private void SetForkIds(List<int> forkIds)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), BinarySerializerHelper.SerializeObject(forkIds), null);
        }

        private Fork GetFork(int id)
        {
            if (id == 0) throw new ArgumentNullException(nameof(id));

            var forkData = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, id), null);

            if (forkData == null)
                throw new ArgumentException($"Fork id:{id} doesn't reference actual fork");

            return ProtoBufSerializerHelper.Deserialize<Fork>(forkData);
        }

        private void SetFork(Fork fork)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, fork.Id), ProtoBufSerializerHelper.Serialize(fork), null);
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, fork.Id), BinarySerializerHelper.SerializeObject(DateTime.UtcNow), null);
        }

        public ForksWrapper<TDataTypesEnum> GetWrapper(int forkId)
        {
            return new ForksWrapper<TDataTypesEnum>(KeyValueStore, AppId, forkId);
        }
    }
}
