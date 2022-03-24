using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class Retry3<TTransport, TSupport> : Retry<TTransport, TSupport>
    where TTransport : ITransport<TSupport>, IPageable, new()
    {
        public Retry3() : base(3)
        {

        }
        public Retry3(TTransport transport) : base(transport, 3)
        {

        }
    }

    public class Retry<TTransport, TSupport> : ITransport<TSupport>, IPageable
    where TTransport : ITransport<TSupport>, IPageable, new()
    {
        private TTransport transport;
        private int retryCount;
        private int retryLeft;

        public Retry(int count = 3)
        : this(new TTransport(), count)
        {
            this.transport = new TTransport();
        }

        public Retry(TTransport transport, int count = 3)
        {
            this.transport = transport;
            this.retryCount = this.retryLeft = count;
        }

        public int Total { get => transport.Total; set => transport.Total = value; }

        public void Dispose()
        {
            this.transport.Dispose();
        }

        public async Task<TSupport> GetStreamAsync()
        {
            try
            {
                var support = await this.transport.GetStreamAsync();
                this.retryLeft = this.retryCount;
                return support;
            }
            catch (Exception)
            {
                retryLeft--;
                if (this.retryLeft > 0)
                    return await this.GetStreamAsync();
                throw;
            }
        }

        public async Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            try
            {
                await this.transport.InitializeAsync(connectionString, parameters);
                this.retryLeft = this.retryCount;
            }
            catch (Exception)
            {
                retryLeft--;
                if (this.retryLeft > 0)
                {
                    await this.InitializeAsync(connectionString, parameters);
                    return;
                }
                throw;
            }
        }

        public async Task NextPage()
        {
            try
            {
                await this.transport.NextPage();
                this.retryLeft = this.retryCount;
            }
            catch (Exception)
            {
                retryLeft--;
                if (this.retryLeft > 0)
                {
                    await this.GetStreamAsync();
                    return;
                }
                throw;
            }
        }
    }
}