using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public interface IPageable
    {
        int Total { get; set; }

        Task NextPage();
    }

    public class PagedTransport<T, TSupport> : ITransport, ITransport<TSupport>, IPageable
    where T : ITransport, ITransport<TSupport>, new()
    {
        private T transport = new T();
        private string offsetParameterName;
        private string countParameterName;
        private string connectionString;

        private int offset, count;
        private KeyValuePair<string, object>[] parameters;

        public PagedTransport(string offsetParameterName, string countParameterName)
        {
            this.offsetParameterName = offsetParameterName;
            this.countParameterName = countParameterName;
        }

        public int Total { get; set; }

        public void Dispose()
        {
            transport.Dispose();
        }

        public Task<TSupport> GetStreamAsync()
        {
            return transport.GetStreamAsync();
        }

        public virtual Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            this.connectionString = connectionString;
            this.parameters = parameters;
            count = Convert.ToInt32(parameters.First(p => p.Key == countParameterName).Value);
            offset = Convert.ToInt32(parameters.First(p => p.Key == offsetParameterName).Value);
            return transport.InitializeAsync(connectionString, parameters);
        }

        public async Task NextPage()
        {
            offset += count;
            if (offset > Total)
            {
                await Task.FromCanceled(new System.Threading.CancellationToken(true));
                return;
            }
            var nextPageTransport = new T();
            await nextPageTransport.InitializeAsync(connectionString, parameters.Select(p =>
            {
                if (p.Key == offsetParameterName)
                    return new KeyValuePair<string, object>(p.Key, offset);
                if (p.Key == countParameterName)
                    return new KeyValuePair<string, object>(p.Key, count);
                return p;
            }).ToArray());
            this.transport = nextPageTransport;
        }
    }



    // public class PagedTransportFromData<TTransport> : IDataProvider, IConfigurable<TreeOptions, Task<IDataReader>>
    // where TTransport : ITransport
    // {
    //     private DataProvider<TreeReader, TreeOptions, TTransport> transport;
    //     private string offsetFieldName;
    //     private string countFieldName;

    //     public Task<IDataReader> Configure(TreeOptions options)
    //     {
    //         return transport.
    //     }

    //     public async Task<System.Data.IDataReader> ExecuteReaderAsync()
    //     {
    //         return new TreePagedReader(await transport.ExecuteReaderAsync());
    //     }

    //     private class TreePagedReader : DataReaderProxy<TreeReader>
    //     {
    //         public TreePagedReader(TreeReader reader)
    //         : base(reader)
    //         {
    //         }

    //         public override bool MoveNext()
    //         {
    //             return base.MoveNext();
    //         }
    //     }
    // }
}