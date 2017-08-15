using KVS.Forks.Core.Entities;
using KVS.Forks.Core.Helpers;
using KVS.Forks.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KVS.Forks.Core
{
    public class ForkProvider<TDataTypesEnum>
    {
        private Dictionary<int, DateTime> _forksTimeStamps = new Dictionary<int, DateTime>();
        private Dictionary<int, Fork> _forks = new Dictionary<int, Fork>();

        private readonly int _appId;
        public int AppId
        {
            get
            {
                return _appId;
            }
        }

        private readonly IKeyValueStore<TDataTypesEnum> _store;
        public IKeyValueStore<TDataTypesEnum> Store
        {
            get
            {
                return _store;
            }
        }

        public ForkProvider(IKeyValueStore<TDataTypesEnum> store, int appId)
        {
            _store = store;
            _appId = appId;

            InitForksDict();

            UpdateForks();
            Task.Factory.StartNew(() => DoWork(), TaskCreationOptions.LongRunning);
        }

        private void InitForksDict()
        {
            var rawForks = new Dictionary<int, ForkRawData>();
            var forkIds = (List<int>)BinarySerializerHelper.DeserializeObject(Store.Get(Store.DefaultType, KeyGenerator.GenerateForksKey(AppId), null));
            foreach (var forkId in forkIds)
            {
                rawForks[forkId] = ProtoBufSerializerHelper.Deserialize<ForkRawData>(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, forkId), null));
                _forksTimeStamps[forkId] = (DateTime)BinarySerializerHelper.DeserializeObject(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, forkId), null));
            }

            CreateFork(rawForks, 1);
        }

        private void CreateFork(Dictionary<int, ForkRawData> rawForks, int forkId)
        {
            var forkRawData = rawForks[forkId];
            var res = new Fork { Id = forkRawData.Id };

            UpdateFork(res, forkRawData);

            _forks[forkRawData.Id] = res;

            if (forkRawData.ParentId != 0)
            {
                if (!_forks.ContainsKey(forkRawData.ParentId))
                {
                    CreateFork(rawForks, forkRawData.ParentId);
                }
                res.Parent = _forks[forkRawData.ParentId];
            }

            foreach (var childId in forkRawData.ChildrenIds)
            {
                if (!_forks.ContainsKey(childId))
                {
                    CreateFork(rawForks, childId);
                }
                res.Children.Add(_forks[childId]);
            }
        }

        private AutoResetEvent _updateFork = new AutoResetEvent(false);
        private TimeSpan _updateForkInterval = TimeSpan.FromSeconds(0.5);

        public Fork GetFork(int forkId)
        {
            return _forks[forkId];
        }

        private void UpdateFork(Fork forkToUpdate, ForkRawData forkRawData)
        {
            forkToUpdate.Name = forkRawData.Name;
            forkToUpdate.Description = forkRawData.Description;
            forkToUpdate.IsInGracePeriod = forkRawData.IsInGracePeriod;
        }

        private void DoWork()
        {
            while (true)
            {
                UpdateForks();
                _updateFork.WaitOne(_updateForkInterval);
            }
        }

        private void UpdateForks()
        {
            var changedForkIds = new HashSet<int>();

            var updatedForkIds = (List<int>)BinarySerializerHelper.DeserializeObject(Store.Get(Store.DefaultType, KeyGenerator.GenerateForksKey(AppId), null));
            var newForkIds = updatedForkIds.Except(_forks.Keys);
            foreach (var newForkId in newForkIds)
            {
                var bytesNewRawFork = Store.Get(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, newForkId), null);
                if (bytesNewRawFork == null)
                    continue;

                var newRawFork = ProtoBufSerializerHelper.Deserialize<ForkRawData>(bytesNewRawFork);

                var bytesNewForkTimeStamp = Store.Get(Store.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, newForkId), null);
                if (bytesNewForkTimeStamp == null)
                    continue;

                var newForkTimeStamp = (DateTime)BinarySerializerHelper.DeserializeObject(bytesNewForkTimeStamp);
                
                var parentFork = newRawFork.ParentId == 0 ? null : _forks[newRawFork.ParentId];
                var newFork = new Fork
                {
                    Id = newForkId,
                    Name = newRawFork.Name,
                    Description = newRawFork.Description,
                    IsInGracePeriod = newRawFork.IsInGracePeriod,
                    Parent = parentFork
                };

                if (parentFork != null)
                {
                    parentFork.Children.Add(newFork);
                    changedForkIds.Add(parentFork.Id);
                }

                _forks[newForkId] = newFork;
                _forksTimeStamps[newForkId] = newForkTimeStamp;

            }
            var toDel = _forks.Keys.Except(updatedForkIds).ToList();
            foreach (var toDelFork in toDel)
            {
                _forks.Remove(toDelFork);
                _forksTimeStamps.Remove(toDelFork);
            }

            foreach (var forkId in _forks.Keys.ToList())
            {
                var newTimeStamp = (DateTime)BinarySerializerHelper.DeserializeObject(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, forkId), null));
                var _currentForkTimeStamp = _forksTimeStamps[forkId];
                if (newTimeStamp > _currentForkTimeStamp)
                {
                    var rawFork = ProtoBufSerializerHelper.Deserialize<ForkRawData>(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, forkId), null));
                    _forksTimeStamps[forkId] = newTimeStamp;

                    UpdateFork(_forks[forkId], rawFork);
                    changedForkIds.Add(forkId);
                }
            }

            RaiseForkChanged(changedForkIds.ToList());
        }

        protected void RaiseForkChanged(List<int> forkIds)
        {
            ForkChanged?.Invoke(this, new ForkChangedEventArgs { ForkIds = forkIds });
        }

        public event EventHandler<ForkChangedEventArgs> ForkChanged;
    }
    public class ForkChangedEventArgs : EventArgs
    {
        public List<int> ForkIds { get; set; }
    }
}