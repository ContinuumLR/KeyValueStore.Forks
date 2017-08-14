using ProtoBuf;
using System;
using System.Collections.Generic;

namespace KVS.Forks.Core.Entities
{
    [ProtoContract]
    public class Fork
    {
        [ProtoMember(1)]
        public virtual int Id { get; set; }

        [ProtoMember(2)]
        public virtual string Name { get; set; }

        [ProtoMember(3)]
        public virtual string Description { get; set; }

        [ProtoMember(4, AsReference = true)]
        public virtual Fork Parent { get; set; }

        [ProtoMember(5)]
        public virtual List<Fork> Children { get; set; } = new List<Fork>();
        
        public bool ReadOnly
        {
            get
            {
                return Children != null && Children.Count > 0;
            }
        }
        
        public List<Fork> GetAllParents()
        {
            if (Parent == null)
                return new List<Fork>();

            var res = new List<Fork>();

            var currentParent = Parent;

            while(currentParent != null)
            {
                res.Add(currentParent);

                currentParent = currentParent.Parent;
            }

            return res;
        }
    }
}