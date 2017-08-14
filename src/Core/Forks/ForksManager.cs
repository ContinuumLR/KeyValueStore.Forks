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

            AppId = appId;
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
            var appIds = KeyValueStore.Get<List<int>>(KeyValueStore.DefaultType, KeyGenerator.AppsKey, null);

            if (appIds == null)
            {
                appIds = new List<int>();
                KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.AppsKey, appIds, null);
            }

            if (appIds.Contains(appId))
                throw new ArgumentException(nameof(appId));

            appIds.Add(appId);

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.AppsKey, appIds, null);

            var res = new App
            {
                Id = appId,
                Name = name,
                Description = description
            };

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateAppKey(appId), ProtoBufSerializerHelper.Serialize(res), null);

            SetApp(appId);

            CreateMasterFork();

            ForkProvider = new ForkProvider<TDataTypesEnum>(KeyValueStore, AppId);
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

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), forkIds, null);
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

            var newFork = new ForkRawData
            {
                Id = id,
                Name = name,
                Description = description,
                ParentId = parentFork.Id,
                IsInGracePeriod = true
            };
            parentFork.ChildrenIds.Add(newFork.Id);
            SetFork(newFork);
            SetFork(parentFork);


            Task.Factory.StartNew(() => HandleGracePeriod(newFork), TaskCreationOptions.LongRunning);
        }

        private void HandleGracePeriod(ForkRawData newFork)
        {
            //Thread.Sleep(TimeSpan.FromSeconds(10));
            var fork = GetFork(newFork.Id);
            fork.IsInGracePeriod = false;

            SetFork(fork);
        }

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

            KeyValueStore.FlushKeys(KeyGenerator.GenerateForkPattern(AppId, id));

            return true;
        }

        private List<int> GetForkIds()
        {
            return KeyValueStore.Get<List<int>>(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), null);
        }

        private void SetForkIds(List<int> forkIds)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), forkIds, null);
        }

        private ForkRawData GetFork(int id)
        {
            if (id == 0) throw new ArgumentNullException(nameof(id));

            var forkData = KeyValueStore.Get<byte[]>(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, id), null);

            if (forkData == null)
                throw new ArgumentException($"Fork id:{id} doesn't reference actual fork");

            return ProtoBufSerializerHelper.Deserialize<ForkRawData>(forkData);

        }

        private void SetFork(ForkRawData fork)
        {
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, fork.Id), ProtoBufSerializerHelper.Serialize(fork), null);
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, fork.Id), DateTime.UtcNow, null);
        }

        public ForksWrapper<TDataTypesEnum> GetWrapper(int forkId)
        {
            return new ForksWrapper<TDataTypesEnum>(KeyValueStore, AppId, forkId);
        }
    }
}