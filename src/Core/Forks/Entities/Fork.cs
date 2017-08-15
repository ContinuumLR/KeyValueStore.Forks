using ProtoBuf;
using System;
using System.Collections.Generic;

namespace KVS.Forks.Core.Entities
{
    public class Fork
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public Fork Parent { get; set; }
        public bool IsInGracePeriod { get; set; }
        public List<Fork> Children { get; set; } = new List<Fork>();
        public bool ReadOnly
        {
            get
            {
                return IsInGracePeriod || Children.Count > 0;
            }
        }
        public List<Fork> GetAllParents()
        {
            if (Parent == null)
                return new List<Fork>();

            var res = new List<Fork>();

            var currentParent = Parent;

            while (currentParent != null)
            {
                res.Add(currentParent);

                currentParent = currentParent.Parent;
            }

            return res;
        }
    }
}