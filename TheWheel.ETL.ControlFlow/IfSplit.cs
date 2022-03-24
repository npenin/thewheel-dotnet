using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.ControlFlow
{
    public class IfSplit : IDataReceiver<Func<IDataRecord, bool>>, IDataReceiver<Func<IDataRecord, IDataRecord>>
    {
        private PassthroughProvider @else;
        private readonly List<Task> tasks = new List<Task>();

        public IfSplit()
        {
            Then = new PassthroughProvider("then");
        }

        public void Await(Task t)
        {
            tasks.Add(t);
        }

        public async Task Await()
        {
            var delay = new Task[1];
            do
            {
                delay[0] = Task.Delay(1000);
                await Task.WhenAny(tasks.ToArray().Union(delay));
                var faulted = tasks.FirstOrDefault(t => t != null && t.IsFaulted);
                if (faulted != null)
                    throw new AggregateException(faulted.Exception);
            }
            while (tasks.Count != tasks.Count(t => t != null && t.IsCompleted));
        }

        public PassthroughProvider Then { get; }
        public PassthroughProvider Else
        {
            get
            {
                if (@else == null)
                    @else = new PassthroughProvider("else");
                return @else;
            }
        }
        public async Task ReceiveAsync(IDataProvider provider, Func<IDataRecord, bool> query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                while (reader.Read())
                {
                    if (query(reader))
                        await Then.Push(reader);
                    else if (@else != null)
                        await @else.Push(reader);
                }
                await Then.Push(null);
                if (@else != null)
                    await @else.Push(null);
            }
        }


        public async Task ReceiveAsync(IDataProvider provider, Func<IDataRecord, IDataRecord> query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                var enumerator = reader as IEnumerator<IDataRecord>;
                if (enumerator != null)
                {
                    while (enumerator.MoveNext() && !token.IsCancellationRequested)
                    {
                        var record = query(enumerator.Current);
                        if (record != null)
                            await Then.Push(record);
                        else if (@else != null)
                            await @else.Push(enumerator.Current);
                    }
                }
                else
                {
                    while (reader.Read() && !token.IsCancellationRequested)
                    {
                        var record = query(reader);
                        if (record != null)
                            await Then.Push(record);
                        else if (@else != null)
                            await @else.Push(reader);
                    }
                }
                await Then.Push(null);
                if (@else != null)
                    await @else.Push(null);
            }
        }
    }
}
