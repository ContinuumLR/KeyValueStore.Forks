using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Helpers
{
    public static class ParamsCastHelper
    {
        public static TParams TryCastParams<TParams>(object extraParams)
            where TParams : class
        {
            if (extraParams == null)
                throw new ArgumentNullException(nameof(extraParams));

            return extraParams as TParams ?? throw new ArgumentException($"Couldn't cast {nameof(extraParams)} to {nameof(TParams)}");
        }
    }
}
