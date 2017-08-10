using System.Collections.Generic;

namespace KVS.Forks.Core.Entities
{
    public class Fork
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public Fork Parent { get; set; }
        public List<Fork> Children { get; set; } = new List<Fork>();
        public bool ReadOnly { get; set; }
    }
}