using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.ServiceBus
{
    public abstract class OneWayMessageBase : MessageBase
    {
        public OneWayMessageBase(string culture, Guid correlationId)
        {
            Culture = culture;
            CorrelationId = correlationId;
        }

        protected OneWayMessageBase()
        {

        }

        public OneWayMessageBase(MessageBase other)
            : base(other)
        {
        }

        protected internal override bool IsOneWay
        {
            get { return true; }
        }

        protected override void Merge(MessageBase message)
        {
            throw new NotSupportedException();
        }

    }
}
