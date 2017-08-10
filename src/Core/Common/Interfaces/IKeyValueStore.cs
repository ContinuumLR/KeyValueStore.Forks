using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Interfaces
{
    public interface IKeyValueStore<TDataTypesEnum>
    {
        bool Set<T>(TDataTypesEnum type, string key, T value, object extraParams );
        bool Set<T>(TDataTypesEnum type, IEnumerable<Tuple<string, T, object>> values);
        T Get<T>(TDataTypesEnum type, string key, object extraParams);
        IDictionary<string, T> Get<T>(TDataTypesEnum type, IEnumerable<Tuple<string, object>> keys);

        TDataTypesEnum DefaultType { get; }
    }
}
