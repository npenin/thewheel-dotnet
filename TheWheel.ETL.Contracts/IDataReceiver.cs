using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public interface IReceiver<TSource, TQuery>
    {
        Task ReceiveAsync(TSource provider, TQuery query, CancellationToken token);

    }

    public interface IDataReceiver<TQuery> : IReceiver<IDataProvider, TQuery>
    {
    }

    public interface ILazyReceiver<TSource>
    {
        Task ReceiveAsync(TSource provider, CancellationToken token);
    }
    public interface ILazyReceiver : ILazyReceiver<IDataProvider>
    {
    }
    // public interface IDataReceiver<TQuery, TTransport> : IDataReceiver<TQuery>
    // where TTransport : ITransport
    // {
    //     TTransport Transport { get; set; }

    //     Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters);
    // }
}