using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    class Transport<TSupport> : ITransport<TSupport>
    {
        private TSupport support;

        public Transport(TSupport support)
        {
            this.support = support;
        }

        public void Dispose()
        {
            if (support is IDisposable disposable)
                disposable.Dispose();
        }

        public Task<TSupport> GetStreamAsync()
        {
            return Task.FromResult(support);
        }

        public Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            return Task.CompletedTask;
        }
    }

    public static class Transport
    {
        public static ITransport<TSupport> From<TSupport>(TSupport support)
        {
            return new Transport<TSupport>(support);
        }
    }
}