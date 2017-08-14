using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVS.Forks.Core.Entities
{
    [ProtoContract]
    public class ForkRawData
    {
        [ProtoMember(1)]
        public virtual int Id { get; set; }

        [ProtoMember(2)]
        public virtual string Name { get; set; }

        [ProtoMember(3)]
        public virtual string Description { get; set; }

        [ProtoMember(4)]
        public virtual int ParentId { get; set; }

        [ProtoMember(5)]
        public virtual bool IsInGracePeriod { get; set; }

        [ProtoMember(6)]
        public virtual List<int> ChildrenIds { get; set; } = new List<int>();

        public bool ReadOnly
        {
            get
            {
                return IsInGracePeriod || ChildrenIds.Count > 0;
            }
        }
    }
}