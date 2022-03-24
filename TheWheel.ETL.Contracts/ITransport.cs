using System;
using System.Threading.Tasks;
using System.Data;
using System.Collections.Generic;
using System.IO;

namespace TheWheel.ETL.Contracts
{
    public interface ITransport : IDisposable
    {
        Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters);
    }

    public interface ITransport<T> : ITransport
    {
        Task<T> GetStreamAsync();
    }
}
