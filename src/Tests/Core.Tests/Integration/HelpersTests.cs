using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core;
using KVS.Forks.Core.Redis.StackExchange.Params;
using KVS.Forks.Core.Helpers;

namespace Core.Tests.Integration
{
    [TestClass]
    public class HelpersTests
    {
        [TestMethod]
        public void ParamsCastHelper_TryCastParams_Exceptions()
        {
            Assert.ThrowsException<ArgumentNullException>(() => ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(null));
            Assert.ThrowsException<ArgumentException>(() => ParamsCastHelper.TryCastParams<StackExchangeRedisHashParams>(123));
        }
    }
}
