using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core;
using KVS.Forks.Core.Redis.StackExchange;
using System.Collections.Generic;

namespace Core.Tests
{
    [TestClass]
    public class CoreTests
    {
        [TestMethod]
        public void ForksWrapper_StringTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
        }

        [TestMethod]
        public void ForksWrapper_HashTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            wrapper.HashSet("hashTestKey1", new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("hashTestKey1_1", 1),
                new KeyValuePair<string, int>("hashTestKey1_2", 2) }.ToArray());
            var res = wrapper.HashGet<int>("hashTestKey1", new string[] { "hashTestKey1_1", "hashTestKey1_2" });

            Assert.AreEqual(1, res["hashTestKey1_1"]);
            Assert.AreEqual(2, res["hashTestKey1_2"]);
        }
    }
}
