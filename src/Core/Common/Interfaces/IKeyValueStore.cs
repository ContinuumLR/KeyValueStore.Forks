using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Interfaces
{
    public interface IKeyValueStore<TDataTypesEnum>
    {
        bool Set(TDataTypesEnum type, string key, byte[] value, object extraParams);
        bool Set(TDataTypesEnum type, IEnumerable<Tuple<string, byte[], object>> values);
        byte[] Get(TDataTypesEnum type, string key, object extraParams);
        IDictionary<string, byte[]> Get(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys);
        bool Delete(TDataTypesEnum type, string key, object extraParams);
        bool Delete(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys);
        bool Exists(TDataTypesEnum type, string key, object extraParams);

        //Key manipulation
        bool FlushKeys(string pattern);
        string[] Keys(string pattern);
        IEnumerable<Tuple<TDataTypesEnum, byte[], object>> GetKeyData(string key);

        TDataTypesEnum DefaultType { get; }
    }
}
