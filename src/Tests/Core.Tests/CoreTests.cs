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
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, fork.Id);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childFork.Id);
            wrapper.StringSet("testKey3", 3);
            var res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childChildFork.Id);
            wrapper.StringSet("testKey3", 3);
            res = wrapper.StringGet<int>(new string[] { "testKey1", "testKey2", "testKey3" });

            Assert.AreEqual(1, res["testKey1"]);
            Assert.AreEqual(2, res["testKey2"]);
            Assert.AreEqual(3, res["testKey3"]);
            Assert.AreEqual(3, res.Count);

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 1, childFork2.Id);
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
            var wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 2, fork.Id);

            wrapper.StringSet(new List<KeyValuePair<string, int>> { new KeyValuePair<string,int>("testKey1", 1),
                new KeyValuePair<string, int>("testKey2", 2) }.ToArray());

            wrapper = new ForksWrapper<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store, 2, childFork.Id);
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


        [TestMethod]
        public void ForksManager_Merge()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            manager.CreateApp(2, "test", "some test app");

            var wrapper = manager.GetWrapper(1);
            wrapper.StringSet("1", 1);

            manager.CreateFork(2, "test2", "some test fork", 1);
            wrapper = manager.GetWrapper(2);
            wrapper.StringSet("2", 2);
            wrapper.StringSet("3", 3);
            wrapper.StringSet("4", 4);

            manager.CreateFork(3, "test2", "some test fork", 2);
            wrapper = manager.GetWrapper(3);

            wrapper.KeyDelete("2");
            wrapper.StringSet("3", 4);
            wrapper.StringSet("5", 5);

            manager.CreateFork(21, "test2", "some test fork", 2);
            wrapper = manager.GetWrapper(21);

            wrapper.KeyDelete("3");
            wrapper.StringSet("2", 2);
            wrapper.StringSet("6", 6);
            wrapper.StringSet("5", 4);

            manager.MergeFork(3, 21);
            wrapper = manager.GetWrapper(100);

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
