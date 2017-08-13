using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using KVS.Forks.Core;
using KVS.Forks.Core.Redis.StackExchange;
using KVS.Forks.Core.Redis.StackExchange.Params;
using static KVS.Forks.Core.Redis.StackExchange.StackExchangeRedisKeyValueStore;
using System.Collections.Generic;

namespace Core.Tests.Integration
{
    [TestClass]
    public class StackExchangeRedisKeyValueStoreTests
    {
        [TestMethod]
        public void StackExchangeRedisKeyValueStore_String()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");

            store.Set(StackExchangeRedisDataTypesEnum.String, "testKey1", 123);
            var res = store.Get<int>(StackExchangeRedisDataTypesEnum.String, "testKey1");

            Assert.AreEqual(123, res);

            store.Set(StackExchangeRedisDataTypesEnum.String, new List<Tuple<string, int, object>>
            {
                Tuple.Create<string, int, object>("testKey1_1", 1, null),
                Tuple.Create<string, int, object>("testKey1_2", 2, null),
                Tuple.Create<string, int, object>("testKey1_3", 3, null)
            });

            var multipleRes = store.Get<int>(StackExchangeRedisDataTypesEnum.String, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey1_1", null),
                Tuple.Create<string, object>("testKey1_2", null),
                Tuple.Create<string, object>("testKey1_3", null)
            });

            Assert.AreEqual(1, multipleRes["testKey1_1"]);
            Assert.AreEqual(2, multipleRes["testKey1_2"]);
            Assert.AreEqual(3, multipleRes["testKey1_3"]);


            store.Delete(StackExchangeRedisDataTypesEnum.String, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey1_1", null),
                Tuple.Create<string, object>("testKey1_2", null)
            });

            multipleRes = store.Get<int>(StackExchangeRedisDataTypesEnum.String, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey1_1", null),
                Tuple.Create<string, object>("testKey1_2", null),
                Tuple.Create<string, object>("testKey1_3", null)
            });

            Assert.AreEqual(3, multipleRes["testKey1_3"]);
            Assert.AreEqual(1, multipleRes.Count);
        }

        [TestMethod]
        public void StackExchangeRedisKeyValueStore_Hash()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");

            store.Set(StackExchangeRedisDataTypesEnum.Hash, "testKey2", 123, new StackExchangeRedisHashParams { HashField = "testKey2Hash" });
            var res = store.Get<int>(StackExchangeRedisDataTypesEnum.Hash, "testKey2", new StackExchangeRedisHashParams { HashField = "testKey2Hash" });

            Assert.AreEqual(123, res);

            store.Set(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, int, object>>
            {
                Tuple.Create<string, int, object>("testKey2_1", 4, new StackExchangeRedisHashParams { HashField = "testKey2_1_1" }),
                Tuple.Create<string, int, object>("testKey2_1", 5, new StackExchangeRedisHashParams { HashField = "testKey2_1_2" }),
                Tuple.Create<string, int, object>("testKey2_1", 6, new StackExchangeRedisHashParams { HashField = "testKey2_1_3" })
            });

            var multipleRes = store.Get<int>(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_1" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_2" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_3" } )
            });

            Assert.AreEqual(4, multipleRes["testKey2_1_1"]);
            Assert.AreEqual(5, multipleRes["testKey2_1_2"]);
            Assert.AreEqual(6, multipleRes["testKey2_1_3"]);


            store.Delete(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_2" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_3" } )
            });
            multipleRes = store.Get<int>(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_1" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_2" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_3" } )
            });

            Assert.AreEqual(4, multipleRes["testKey2_1_1"]);
            Assert.AreEqual(1, multipleRes.Count);

            store.Delete(StackExchangeRedisDataTypesEnum.Hash, "testKey2_1", null);
            multipleRes = store.Get<int>(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, object>>
            {
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_1" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_2" } ),
                Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_3" } )
            });
            Assert.AreEqual(0, multipleRes.Count);
        }

        [TestMethod]
        public void StackExchangeRedisKeyValueStore_Hash_Exceptions()
        {
            var store = new StackExchangeRedisKeyValueStore("localhost:6379");

            Assert.ThrowsException<ArgumentException>(() =>
            {
                store.Set(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, int, object>>
                {
                    Tuple.Create<string, int, object>("testKey2_1", 1, new StackExchangeRedisHashParams { HashField = "testKey2_1_1" }),
                    Tuple.Create<string, int, object>("testKey2_2", 2, new StackExchangeRedisHashParams { HashField = "testKey2_1_2" })
                });
            });

            Assert.ThrowsException<ArgumentException>(() =>
            {
                store.Get<int>(StackExchangeRedisDataTypesEnum.Hash, new List<Tuple<string, object>>
                {
                    Tuple.Create<string, object>("testKey2_1", new StackExchangeRedisHashParams { HashField = "testKey2_1_1" }),
                    Tuple.Create<string, object>("testKey2_2", new StackExchangeRedisHashParams { HashField = "testKey2_1_2" })
                });
            });
        }
    }
}
