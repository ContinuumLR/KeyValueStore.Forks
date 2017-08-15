using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.DTOs
{
    public class Fork
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public List<Fork> Children { get; set; } = new List<Fork>();
        public bool ReadOnly { get; set; }
    }
}
