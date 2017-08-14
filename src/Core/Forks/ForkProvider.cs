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
        private readonly int _forkId;
        private int ForkId
        {
            get
            {
                return _forkId;
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

        private readonly IKeyValueStore<TDataTypesEnum> _store;
        public IKeyValueStore<TDataTypesEnum> Store
        {
            get
            {
                return _store;
            }
        }

        public ForkProvider(IKeyValueStore<TDataTypesEnum> store, int appId, int forkId)
        {
            _store = store;
            _forkId = forkId;
            _appId = appId;
            UpdateCurrentFork();
            Task.Factory.StartNew(() => DoWork(), TaskCreationOptions.LongRunning);
        }

        private AutoResetEvent _updateFork = new AutoResetEvent(false);
        private TimeSpan _updateForkInterval = TimeSpan.FromSeconds(10);

        private DateTime _currentForkTimeStamp = DateTime.MinValue;
        private Fork _currentFork;
        public Fork CurrentFork
        {
            get
            {
                return _currentFork;
            }
        }

        private void DoWork()
        {
            while (true)
            {
                UpdateCurrentFork();
                _updateFork.WaitOne(_updateForkInterval);
            }
        }

        private void UpdateCurrentFork()
        {
            var newTimeStamp = Store.Get<DateTime>(Store.DefaultType, KeyGenerator.GenerateForkTimeStampKey(AppId, ForkId), null);

            if (newTimeStamp > _currentForkTimeStamp)
            {
                _currentFork = ProtoBufSerializerHelper.Deserialize<Fork>(Store.Get<byte[]>(Store.DefaultType, KeyGenerator.GenerateForkKey(AppId, ForkId), null));
                _currentForkTimeStamp = newTimeStamp;
                RaiseForkChanged();
            }
        }

        protected void RaiseForkChanged()
        {
            ForkChanged?.Invoke(this, new EventArgs());
        }

        public event EventHandler<EventArgs> ForkChanged;
    }
}