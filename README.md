# KeyValueStore.Forks
Fork your KV store's data with minimal memory footprint.
We try to make it as performant as possible considering some performance is the tradeoff for the low memory footprint when working on 2 forks of the same data when data is changed over time.

# Usage
Using the console tool - create app, choose your key value store implementation (Currently only redis is implemented, but it is easy to implement more wrappers for other engines), create a FroksWrapper with a forkId and start using the wrapper as your data source.

Examples comming soon.

# Features
* StackExchange.Redis wrapper implemetation for Set/Get with String/Hash data types
* Set/Get/Delete wrapped based on forks, currently a parent fork is Readonly while leaf forks are not.
* ForkManager (including apis + console utility)
  * CreateApp - (also creates master fork)
  * CreateFork
  * DeleteFork - only when not readonly (flushes all keys, and removes from the KVSF listings)
  * MergeFork - merges fork's data into another fork, creating a new child fork in the process in order not to lose data.
  * PruneFork - in order to clean history of forks, creates a new master fork from an origin fork.
  * Common usage apis - GetMasterForks, GetApps

# Future features
* Allow working on parent fork (Complex Locking in store).
* Delete fork when children exist (Merge keys from deleted fork into children / Also delete children)
* ForkStatusEnum (Controlling when a fork is available for different uses - will replace Readonly flag)

# License
KeyValueStore.Forks is licensed as AGPL software.

AGPL is a free / open source software license.

This doesn't mean the software is gratis!

Buying a license is mandatory as soon as you develop commercial activities distributing the KeyValueStore.Forks software inside your product or deploying it on a network without disclosing the source code of your own applications under the AGPL license.

Contact us for more info
