using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public interface IDataProvider
    {
        Task<IDataReader> ExecuteReaderAsync(CancellationToken token);

    }
    public interface IAsyncQueryable<TQuery> : IDataProvider
    {
        Task QueryAsync(TQuery query, CancellationToken token);
    }
    public interface IAsyncNewQueryable<TQuery> : IDataProvider
    {
        Task<IDataProvider> QueryNewAsync(TQuery query, CancellationToken token);
    }
    public interface ITransportable<TTransport>
    {
        TTransport Transport { get; }
    }

    // public interface IDataProvider<TQuery, TTransport> : IDataProviderByQuery<TQuery>, IDataProviderOnTransport<TTransport>
    // where TTransport : ITransport
    // {
    // }
}