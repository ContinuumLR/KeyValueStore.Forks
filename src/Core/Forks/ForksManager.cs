using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), forkIds, null);
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, 1), ProtoBufSerializerHelper.Serialize(masterFork), null);
        }

        public void CreateFork(int id, string name, string description, int parentForkId)
        {
            if (parentForkId == 0) throw new ArgumentNullException(nameof(parentForkId));

            var parentForkData = KeyValueStore.Get<byte[]>(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId,parentForkId), null);

            if (parentForkData == null)
                throw new ArgumentException($"{parentForkId} doesn't reference actual fork");

            var parentFork = ProtoBufSerializerHelper.Deserialize<Fork>(parentForkData);

            var forkIds = KeyValueStore.Get<List<int>>(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), null);
            
            if (forkIds.Contains(id))
                throw new ArgumentException(nameof(id));

            forkIds.Add(id);

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForksKey(AppId), forkIds, null);
            
            var newFork = new Fork
            {
                Id = id,
                Name = name,
                Description = description,
                Parent = parentFork
            };

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, id), ProtoBufSerializerHelper.Serialize(newFork), null);

            parentFork.Children.Add(newFork);

            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkKey(AppId, parentFork.Id), ProtoBufSerializerHelper.Serialize(parentFork), null);

            //Set timestamps
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, id), DateTime.UtcNow, null);
            KeyValueStore.Set(KeyValueStore.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, parentFork.Id), DateTime.UtcNow, null);
        }
        
        public ForksWrapper<TDataTypesEnum> GetWrapper(int forkId)
        {
            return new ForksWrapper<TDataTypesEnum>(KeyValueStore, AppId, forkId);
        }
    }
}
