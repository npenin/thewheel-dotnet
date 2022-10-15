using System;
using System.Threading.Tasks;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace TheWheel.ETL.Contracts
{
    public interface ITransport : IDisposable
    {
        Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters);
    }

    public interface ITransport<T> : ITransport
    {
        Task<T> GetStreamAsync(CancellationToken token);
    }
}
