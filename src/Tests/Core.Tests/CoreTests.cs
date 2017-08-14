using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core;
using KVS.Forks.Core.Redis.StackExchange;
using System.Collections.Generic;
using KVS.Forks.Core.Entities;
using System.Threading;

namespace Core.Tests
{
    [TestClass]
    public class CoreTests
    {
        [TestMethod]
        public void ForksWrapper_ForkGetSetTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);
            manager.CreateApp(2, "GetSetTest", string.Empty);

            manager.CreateFork(2, "test1", "some test fork", 1);
            var wrapper = manager.GetWrapper(2);
            
            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            manager.CreateFork(3, "test2", "some test fork", 2);
                        
            wrapper = manager.GetWrapper(3);
            wrapper.StringSet("testKey3", 3);
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            manager.CreateFork(4, "test3", "some test fork", 2);

            wrapper = manager.GetWrapper(4);
            wrapper.StringSet("testKey3", 3);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            manager.CreateFork(5, "test4", "some test fork", 2);
            wrapper = manager.GetWrapper(5);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(2, res.Count);
        }

        [TestMethod]
        public void ForksWrapper_ForkDeleteTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);
            manager.CreateApp(3, "ForkDeleteTest", string.Empty);

            manager.CreateFork(2, "test1", "some test fork", 1);

            var wrapper = manager.GetWrapper(2);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            manager.CreateFork(3, "test2", "some test fork", 2);
            wrapper = manager.GetWrapper(3);
            wrapper.StringSet("testKey3", 3);
            wrapper.StringSet("testKey2", 4);

            wrapper.KeyDelete(new string[] { "testKey2", "testKey3" });

            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(1, res.Count);
        }

        [TestMethod]
        public void ForksManager_CreateApp()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            manager.CreateApp(1, "test", "some test app");

            var masterWrapper = manager.GetWrapper(1);

            manager.CreateFork(2, "test2", "some test fork", 1);

            manager.GetWrapper(2);

            manager.CreateFork(3, "test3", "some test fork", 1);
            manager.CreateFork(4, "test4", "some test fork", 2);

            manager.DeleteFork(4);
            manager.DeleteFork(3);
            manager.DeleteFork(2);
        }
    }
}