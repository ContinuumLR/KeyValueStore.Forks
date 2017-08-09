using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core.Impl;
using KVS.Forks.Core.Extensions;
using KVS.Forks.Core;

namespace Core.Tests
{
    [TestClass]
    public class CoreTests
    {
        [TestMethod]
        public void ForksWrapperTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            wrapper.StringSet("testKey1", 1);
            var res = wrapper.StringGet<int>("testKey1");

            Assert.AreEqual(1, res);
        }
    }
}
