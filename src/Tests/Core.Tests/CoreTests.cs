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
            store.FlushKeys("KVSF*");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);
            manager.CreateApp(2, "GetSetTest", string.Empty);

            var forkId = manager.CreateFork("test1", "some test fork", 1);
            var wrapper = manager.GetWrapper(forkId);
            
            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            forkId = manager.CreateFork("test2", "some test fork", 2);
                        
            wrapper = manager.GetWrapper(forkId);
            wrapper.StringSet("testKey3", 3);
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            forkId = manager.CreateFork("test3", "some test fork", 2);

            wrapper = manager.GetWrapper(forkId);
            wrapper.StringSet("testKey3", 3);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            forkId = manager.CreateFork("test4", "some test fork", 2);
            wrapper = manager.GetWrapper(forkId);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(2, res.Count);
        }

        [TestMethod]
        public void ForksWrapper_ForkDeleteTest()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            store.FlushKeys("KVSF*");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);
            manager.CreateApp(3, "ForkDeleteTest", string.Empty);

            var forkId = manager.CreateFork("test1", "some test fork", 1);

            var wrapper = manager.GetWrapper(forkId);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            forkId = manager.CreateFork("test2", "some test fork", 2);
            wrapper = manager.GetWrapper(forkId);
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
            store.FlushKeys("KVSF*");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            manager.CreateApp(1, "test", "some test app");

            var masterWrapper = manager.GetWrapper(1);

            var forkId = manager.CreateFork("test2", "some test fork", 1);

            manager.GetWrapper(2);

            var forkId2 = manager.CreateFork("test3", "some test fork", 1);
            var forkId3 = manager.CreateFork("test4", "some test fork", 2);

            manager.DeleteFork(forkId3);
            manager.DeleteFork(forkId2);
            manager.DeleteFork(forkId);
        }


        [TestMethod]
        public void ForksManager_Merge()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");

            store.FlushKeys("KVSF*");

            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);
            
            manager.CreateApp(2, "test", "some test app");

            var wrapper = manager.GetWrapper(1);
            wrapper.StringSet("1", 1);

            var forkId = manager.CreateFork("test2", "some test fork", 1);
            wrapper = manager.GetWrapper(forkId);
            wrapper.StringSet("2", 2);
            wrapper.StringSet("3", 3);
            wrapper.StringSet("4", 4);

            forkId = manager.CreateFork("test2", "some test fork", 2);
            wrapper = manager.GetWrapper(forkId);

            wrapper.KeyDelete("2");
            wrapper.StringSet("3", 4);
            wrapper.StringSet("5", 5);

            var forkId2 = manager.CreateFork("test2", "some test fork", 2);
            wrapper = manager.GetWrapper(forkId2);

            wrapper.KeyDelete("3");
            wrapper.StringSet("2", 2);
            wrapper.StringSet("6", 6);
            wrapper.StringSet("5", 4);

            var newForkId = manager.MergeFork(forkId, forkId2);
            wrapper = manager.GetWrapper(newForkId);

            var values = wrapper.StringGet<int>(new string[] { "1", "2", "3", "4", "5", "6" });

            Assert.AreEqual(5, values.Count);
            Assert.AreEqual(1, values["1"]);
            Assert.AreEqual(4, values["3"]);
            Assert.AreEqual(4, values["4"]);
            Assert.AreEqual(5, values["5"]);
            Assert.AreEqual(6, values["6"]);

        }
    }
}