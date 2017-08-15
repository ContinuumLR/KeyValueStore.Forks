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

            var masterForks = rawForks.Where(x => x.Value.ParentId == 0).Select(x => x.Key).ToArray();
            foreach (var masterFork in masterForks)
            {
                CreateFork(rawForks, masterFork);
            }
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
        private TimeSpan _updateForkInterval = TimeSpan.FromSeconds(1);

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
            foreach (var forkId in _forks.Keys.ToList())
            {
                var newTimeStamp = (DateTime)BinarySerializerHelper.DeserializeObject(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, forkId), null));
                var _currentForkTimeStamp = _forksTimeStamps[forkId];
                if (newTimeStamp == default(DateTime))
                {
                    _forks.Remove(forkId);
                }
                else
                {
                    if (newTimeStamp > _currentForkTimeStamp)
                    {
                        var rawFork = ProtoBufSerializerHelper.Deserialize<ForkRawData>(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, forkId), null));
                        _forksTimeStamps[forkId] = newTimeStamp;

                        var fork = _forks[forkId];

                        if (fork.Children.Count != rawFork.ChildrenIds.Count)
                        {
                            var newForksIds = rawFork.ChildrenIds.Except(fork.Children.Select(x => x.Id)).ToList();
                            foreach (var newForkId in newForksIds)
                            {
                                var rawChildFork = ProtoBufSerializerHelper.Deserialize<ForkRawData>(Store.Get(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, newForkId), null));
                                var newFork = new Fork
                                {
                                    Id = newForkId,
                                    Name = rawChildFork.Name,
                                    Description = rawChildFork.Description,
                                    IsInGracePeriod = rawChildFork.IsInGracePeriod,
                                    Parent = fork
                                };
                                _forks[newForkId] = newFork;
                                _forksTimeStamps[newForkId] = newTimeStamp;
                            }
                        }

                        UpdateFork(fork, rawFork);
                        RaiseForkChanged(forkId);
                    }
                }
            }
        }

        protected void RaiseForkChanged(int forkId)
        {
            ForkChanged?.Invoke(this, new ForkChangedEventArgs { ForkId = forkId });
        }

        public event EventHandler<ForkChangedEventArgs> ForkChanged;
    }
    public class ForkChangedEventArgs : EventArgs
    {
        public int ForkId { get; set; }
    }
}