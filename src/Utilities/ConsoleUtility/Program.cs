using KVS.Forks.Core;
using KVS.Forks.Core.DTOs;
using KVS.Forks.Core.Redis.StackExchange;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleUtility
{
    class Program
    {
        private static int? AppId { get; set; }

        static void Main(string[] args)
        {
            var store = new StackExchangeRedisKeyValueStore(ConfigurationManager.AppSettings["KeyValueConnectionString"]);
            var manager = new ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum>(store);

            while (true)
            {
                if (!AppId.HasValue)
                    ManageApp(manager);

                ManageForks(manager);
            }
        }

        private static void ManageForks(ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum> manager)
        {
            Console.Clear();
            DisplayForks(manager);

            Console.WriteLine("Manage forks");
            Console.WriteLine("1 - Create new fork");
            Console.WriteLine("2 - Merge forks");
            Console.WriteLine("3 - Prune fork");
            Console.WriteLine("4 - Delete fork");

            var input = Console.ReadLine();
            if (string.IsNullOrEmpty(input))
                return;

            var choice = int.Parse(input);
            
            switch (choice)
            {
                case 1:
                    Console.WriteLine("New Name:");
                    var name = Console.ReadLine();

                    Console.WriteLine("Description:");
                    var description = Console.ReadLine();

                    Console.WriteLine("Parent fork Id (keep empty for new master fork):");
                    var parentForkIdString = Console.ReadLine();

                    int? parentFork = null;
                    if (!string.IsNullOrEmpty(parentForkIdString))
                    {
                        parentFork = int.Parse(parentForkIdString);
                    }

                    try
                    {
                        var newForkId = manager.CreateFork(name, description, parentFork);
                        Console.WriteLine($"Fork {newForkId} was created");
                        Thread.Sleep(500);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                        Thread.Sleep(500);
                    }
                    return;
                case 2:
                    DisplayForks(manager);

                    Console.WriteLine("Choose origin fork id");
                    var originForkIdString = Console.ReadLine();

                    if (string.IsNullOrEmpty(originForkIdString))
                        return;

                    int originForkId = int.Parse(originForkIdString);
                    
                    Console.WriteLine("Choose target fork id");
                    var targetForkIdString = Console.ReadLine();
                    
                    if (string.IsNullOrEmpty(targetForkIdString))
                        return;

                    int targetForkId = int.Parse(targetForkIdString);

                    try
                    {
                        var newForkId = manager.MergeFork(originForkId, targetForkId);
                        Console.WriteLine($"Merged Fork {newForkId} was created");
                        Thread.Sleep(500);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                        Thread.Sleep(500);
                    }

                    return;
                case 3:
                    DisplayForks(manager);

                    Console.WriteLine("Choose fork id to prune (create new master)");
                    var pruneForkIdString = Console.ReadLine();

                    if (string.IsNullOrEmpty(pruneForkIdString))
                        return;

                    int pruneForkId = int.Parse(pruneForkIdString);
                    
                    try
                    {
                        var newForkId = manager.PruneForks(pruneForkId);
                        Console.WriteLine($"New master fork {newForkId} was created from fork {pruneForkId}");
                        Thread.Sleep(500);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                        Thread.Sleep(500);
                    }

                    return;
                case 4:
                    DisplayForks(manager);

                    Console.WriteLine("Choose fork id to delete (Only leaves)");
                    var deleteForkIdString = Console.ReadLine();

                    if (string.IsNullOrEmpty(deleteForkIdString))
                        return;

                    int deleteForkId = int.Parse(deleteForkIdString);

                    try
                    {
                        var deleted = manager.DeleteFork(deleteForkId);
                        var not = deleted ? " " : " not ";
                        Console.WriteLine($"Fork {deleteForkId} was{not}deleted");
                        Thread.Sleep(500);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                        Thread.Sleep(500);
                    }

                    return;
                default:
                    break;
            }

        }

        private static void DisplayForks(ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum> manager)
        {
            Console.WriteLine("Forks");
            var masterForks = manager.GetMasterForks();

            foreach (var fork in masterForks)
            {
                DisplayFork(fork, 0);
            }

            Console.WriteLine();
        }

        private static void DisplayFork(Fork fork, int level)
        {
            var tabs = "";
            for (int i = 0; i < level; i++)
            {
                tabs += "   ";
            }

            Console.WriteLine($"{tabs}{fork.Id} - {fork.Name}:{fork.Description}");
            foreach (var child in fork.Children)
            {
                DisplayFork(child, level + 1);
            }
        }

        private static void ManageApp(ForksManager<StackExchangeRedisKeyValueStore.StackExchangeRedisDataTypesEnum> manager)
        {
            Console.Clear();
            Console.WriteLine("Please select an application / create a new one");

            var apps = manager.GetApps();

            Console.WriteLine("0 - create new app");

            foreach (var app in apps)
            {
                Console.WriteLine($"{app.Id} - {app.Name}:{app.Description}");
            }

            var input = Console.ReadLine();

            if (string.IsNullOrEmpty(input))
                return;

            int choice = int.Parse(input);

            if (choice == 0)
            {
                Console.WriteLine("New Id:");

                input = Console.ReadLine();

                int id = int.MinValue;
                int.TryParse(input, out id);

                if (id == int.MinValue)
                    return;

                Console.WriteLine("New Name:");
                var name = Console.ReadLine();

                Console.WriteLine("Description:");
                var description = Console.ReadLine();

                try
                {
                    manager.CreateApp(id, name, description);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    return;
                }

                AppId = id;
            }
            else
            {
                manager.SetApp(choice);
                AppId = choice;
            }
        }
    }
}
