using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core;
using KVS.Forks.Core.Redis.StackExchange;
using System.Collections.Generic;
using KVS.Forks.Core.Entities;

namespace Core.Tests
{
    [TestClass]
    public class CoreTests
    {
        [TestMethod]
        public void ForksWrapper_ForkGetSetTest()
        {
            var fork = new Fork
            {
                Id = 1
            };

            var childFork = new Fork
            {
                Id = 2,
                Parent = fork
            };
            fork.Children.Add(childFork);

            var childFork2 = new Fork
            {
                Id = 4,
                Parent = fork
            };
            fork.Children.Add(childFork2);

            var childChildFork = new Fork
            {
                Id = 3,
                Parent = childFork
            };
            childFork.Children.Add(childChildFork);

            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, fork);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childFork);
            wrapper.StringSet("testKey3", 3);
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childChildFork);
            wrapper.StringSet("testKey3", 3);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childFork2);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(2, res.Count);
        }

        [TestMethod]
        public void ForksWrapper_ForkDeleteTest()
        {
            var fork = new Fork
            {
                Id = 1
            };

            var childFork = new Fork
            {
                Id = 2,
                Parent = fork
            };
            fork.Children.Add(childFork);
            
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 2, fork);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 2, childFork);
            wrapper.StringSet("testKey3", 3);
            wrapper.StringSet("testKey2", 4);

            wrapper.KeyDelete(new string[] { "testKey2", "testKey3" });
            
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });
            
            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(1, res.Count);
        }
    }
}
