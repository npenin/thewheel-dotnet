using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace TheWheel.ETL.Contracts
{
    public class TreeSplit : IDataReceiver<TreeSplitOptions>
    {

        public async Task ReceiveAsync(IAsyncQueryable<TreeOptions> provider, TreeSplitOptions query, CancellationToken token)
        {
            await provider.QueryAsync(query, token);
            if (query.Receivers.Length > query.Matchers.Length)
                throw new InvalidOperationException("You have specified more receivers than Roots");
            var providers = query.Matchers.Select(m => new PassthroughProvider(m.Root)).ToArray();
            List<Task> tasks = new List<Task>();
            for (int i = 0; i < query.Receivers.Length; i++)
            {
                if (query.Receivers[i] != null)
                    tasks.Add(query.Receivers[i].ReceiveAsync(providers[i], token));
            }
            using (var reader = (IMultiDataReader)await provider.ExecuteReaderAsync(token))
            {
                while (reader.MoveNext() && !token.IsCancellationRequested)
                {
                    await providers[reader.OutputIndex].Push(reader.Current);
                }
            }
            foreach (var p in providers)
            {
                await p.Push(null);
            }
            while (tasks.Any(t => t.Status != TaskStatus.RanToCompletion))
            {
                await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(1000));
            }
        }
        public Task ReceiveAsync(IDataProvider provider, TreeSplitOptions query, CancellationToken token)
        {
            return ReceiveAsync((IAsyncQueryable<TreeOptions>)provider, query, token);
        }
    }

    public class TreeSplitOptions : TreeOptions
    {
        public TreeSplitOptions AddMatch(string root, ILazyReceiver<IDataProvider> receiver, params TreeLeaf[] paths)
        {
            base.AddMatch(root, paths);
            var receivers = new ILazyReceiver<IDataProvider>[Matchers.Length];
            if (this.Receivers != null)
                Array.Copy(Receivers, receivers, Receivers.Length);
            receivers[receivers.Length - 1] = receiver;
            Receivers = receivers;
            return this;
        }

        internal ILazyReceiver<IDataProvider>[] Receivers;
    }
}