using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.ServiceBus
{
    public abstract class Scheduler<TMessage>
        where TMessage : MessageBase
    {
        private class SchedulerClient : ServiceBusClient<TMessage>
        {
            private readonly Scheduler<TMessage> scheduler;

            public SchedulerClient(Scheduler<TMessage> scheduler)
            {
                this.scheduler = scheduler;
            }

            protected override Task Handle(TMessage tMessage)
            {
                throw new NotSupportedException();
            }

            protected override void Init(TMessage m)
            {
                scheduler.Init(m);
            }
        }

        SchedulerClient client;
        private bool stop;
        private TimeSpan interval;
        private bool processing;

        public Scheduler()
        {
            client = new SchedulerClient(this);
        }

        protected abstract void Init(TMessage m);

        protected abstract Task Handle(IEnumerable<TMessage> messages);

        public void Start(TimeSpan interval, string connectionString)
        {
            stop = false;
            this.interval = interval;
            client.connection.ConnectionString = connectionString;
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Process(null);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        public void StartOnce(string connectionString)
        {
            stop = false;
            this.interval = TimeSpan.FromSeconds(1);
            client.connection.ConnectionString = connectionString;
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Process(null);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            stop = true;
        }

        public void Stop()
        {
            stop = true;
        }

        private async Task Process(Task t)
        {
            if (stop)
                return;
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Delay(interval).ContinueWith<object>(Process);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            if (processing)
                return;
            processing = true;
            await Handle(client.GetMessages());
            processing = false;
        }
    }
}
