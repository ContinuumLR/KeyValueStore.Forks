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
        public IKeyValueStore<TDataTypesEnum> KeyValueStore { get; private set; }
        public ForkProvider<TDataTypesEnum> ForkProvider { get; private set; }

        public int AppId { get; private set; }

        public ForksManager(IKeyValueStore<TDataTypesEnum> keyValueStore)
        {
            KeyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
        }

        public ForksManager(IKeyValueStore<TDataTypesEnum> keyValueStore, int appId)
        {
            KeyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));

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
            var bytesAppIds = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.AppsKey, null);
            List<int> appIds = null;

            if (bytesAppIds != null)
            {
                appIds = (List<int>)BinarySerializerHelper.DeserializeObject(bytesAppIds);
            }

            if (appIds == null || !appIds.Contains(appId))
                throw new ArgumentException($"{nameof(appId)} - doesn't exist");

            AppId = appId;

            ForkProvider = new ForkProvider<TDataTypesEnum>(KeyValueStore, AppId);
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
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(appId), BinarySerializerHelper.SerializeObject(new List<int>()), null);

            SetApp(appId);

            CreateMasterFork();
        }

        public List<DTOs.App> GetApps()
        {
            var bytesAppIds = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.AppsKey, null);
            List<int> appIds = null;

            if (bytesAppIds == null)
                return new List<DTOs.App>();

            appIds = (List<int>)BinarySerializerHelper.DeserializeObject(bytesAppIds);

            var res = new List<DTOs.App>();

            foreach (var appId in appIds)
            {
                var bytesApp = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.GenerateAppKey(appId), null);
                if (bytesApp == null)
                    continue;

                var app = ProtoBufSerializerHelper.Deserialize<App>(bytesApp);

                res.Add(new DTOs.App
                {
                    Id = app.Id,
                    Name = app.Name,
                    Description = app.Description
                });
            }

            return res;
        }

        private void CreateMasterFork()
        {
            var forkIds = new List<int>();
            forkIds.Add(1);

            var masterFork = new ForkRawData
            {
                Id = 1,
                Name = "master"
            };

            SetForkIds(forkIds);
            SetFork(masterFork);
        }

        /// <summary>
        /// Create a new fork
        /// </summary>
        /// <param name="name"></param>
        /// <param name="description"></param>
        /// <param name="parentForkId">If parent is null, a new master fork will be created</param>
        /// <returns>New fork id</returns>
        public int CreateFork(string name, string description, int? parentForkId = null)
        {
            var forkIds = GetForkIds();
            var newId = forkIds.Max() + 1;

            forkIds.Add(newId);
            SetForkIds(forkIds);

            var newFork = new ForkRawData
            {
                Id = newId,
                Name = name,
                Description = description,
                ParentId = parentForkId ?? 0,
                IsInGracePeriod = true
            };

            if (parentForkId.HasValue)
            {
                var parentFork = GetFork(parentForkId.Value);

                parentFork.ChildrenIds.Add(newFork.Id);
                SetFork(parentFork);
            }
            SetFork(newFork);

            HandleGracePeriod(newFork);

            return newId;
        }

        private void HandleGracePeriod(ForkRawData newFork)
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));

            var fork = GetFork(newFork.Id);
            fork.IsInGracePeriod = false;
            SetFork(fork);

            Thread.Sleep(TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// Deletes a fork, only leaves can be deleted
        /// </summary>
        /// <param name="id">Fork id to delete</param>
        /// <returns>success</returns>
        public bool DeleteFork(int id)
        {
            var forkIds = GetForkIds();

            if (!forkIds.Contains(id))
                throw new ArgumentException(nameof(id));

            var fork = GetFork(id);

            if (fork.ReadOnly)
                return false;

            if (fork.ParentId != 0)
            {
                var parent = GetFork(fork.ParentId);
                parent.ChildrenIds.Remove(fork.Id);
                SetFork(parent);
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

            var currentFork = ForkProvider.GetFork(originForkId);
            var targetFork = ForkProvider.GetFork(targetForkId);

            while (!IsCommonParent(currentFork, targetFork))
            {
                if (currentFork == null)
                    throw new ArgumentException($"No common parents between {originForkId} and {targetForkId}");

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
                        keysToDelete.Add(originalKey.Substring(0, originalKey.Length - KeyGenerator.NullKeyPostFix.Length), type);
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

            var newForkId = CreateFork($"{targetFork.Name} Merge", "", targetFork.Id);

            var wrapper = GetWrapper(newForkId);

            foreach (var keyToDelete in keysToDelete)
                wrapper.Delete(keyToDelete.Value, keyToDelete.Key);

            foreach (var value in valuesToSet)
            {
                wrapper.Set(value.Item2, value.Item1, value.Item3, value.Item4);
            }

            return newForkId;
        }

        private bool IsCommonParent(Fork fork, Fork targetFork)
        {
            var currentFork = targetFork;

            while (currentFork != null)
            {
                if (currentFork.Id == fork.Id)
                    return true;

                currentFork = currentFork.Parent;
            }

            return false;
        }

        /// <summary>
        /// Create new master fork with the data from an exsiting fork
        /// This is used to prune unused forks
        /// The data is trasferred to a new master fork to allow checking the data before deleting the old fork tree.
        /// </summary>
        /// <param name="forkId">Fork to prune</param>
        /// <returns>New master fork id with the data from the old fork</returns>
        public int PruneForks(int forkId)
        {
            var usedKeys = new HashSet<string>();
            var deletedKeys = new HashSet<string>();
            var valuesToSet = new List<Tuple<string, TDataTypesEnum, byte[], object>>();

            var fork = ForkProvider.GetFork(forkId);
            var currentFork = fork;

            while (currentFork != null)
            {
                var forkPattern = KeyGenerator.GenerateForkValuePattern(AppId, currentFork.Id);
                var keys = KeyValueStore.Keys($"{forkPattern}*");

                foreach (var key in keys)
                {
                    var originalKey = key.Substring(forkPattern.Length);

                    // If the key was used/deleted in a lower fork, this key is not relevant
                    if (usedKeys.Contains(originalKey) || deletedKeys.Contains(originalKey))
                        continue;

                    if (originalKey.EndsWith(KeyGenerator.NullKeyPostFix))
                        deletedKeys.Add(originalKey.Substring(0, originalKey.Length - KeyGenerator.NullKeyPostFix.Length));
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

            var newForkId = CreateFork("master", $"Pruned from {fork.Id}:{fork.Name}");

            var wrapper = GetWrapper(newForkId);

            foreach (var value in valuesToSet)
            {
                wrapper.Set(value.Item2, value.Item1, value.Item3, value.Item4);
            }

            return newForkId;
        }

        private List<int> GetForkIds()
        {
            return (List<int>)BinarySerializerHelper.DeserializeObject(KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), null));
        }

        public List<DTOs.Fork> GetMasterForks()
        {
            return ForkProvider.GetMasterForks().Select(x => MapToDto(x)).ToList();
        }

        private DTOs.Fork MapToDto(Fork fork)
        {
            return new DTOs.Fork
            {
                Id = fork.Id,
                Name = fork.Name,
                Description = fork.Description,
                ReadOnly = fork.ReadOnly,
                Children = fork.Children.Select(x => MapToDto(x)).ToList()
            };
        }

        private void SetForkIds(List<int> forkIds)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), BinarySerializerHelper.SerializeObject(forkIds), null);
        }

        private ForkRawData GetFork(int id)
        {
            if (id == 0) throw new ArgumentNullException(nameof(id));

            var forkData = KeyValueStore.Get(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, id), null);

            if (forkData == null)
                throw new ArgumentException($"Fork id:{id} doesn't reference actual fork");

            return ProtoBufSerializerHelper.Deserialize<ForkRawData>(forkData);
        }

        private void SetFork(ForkRawData fork)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, fork.Id), ProtoBufSerializerHelper.Serialize(fork), null);
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, fork.Id), BinarySerializerHelper.SerializeObject(DateTime.UtcNow), null);
        }

        public ForksWrapper<TDataTypesEnum> GetWrapper(int forkId)
        {
            return new ForksWrapper<TDataTypesEnum>(KeyValueStore, AppId, forkId, ForkProvider);
        }
    }
}