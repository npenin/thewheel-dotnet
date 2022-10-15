using System;
using System.Data;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Azure
{
    public class Table : DataProvider<TableReader, TableQuery, ITransport<HttpContent>>
    {
    }

    public class TableReader : TheWheel.ETL.Contracts.DataReader, IConfigurableAsync<TableQuery, IDataReader>
    {
        public TableReader() : base(typeof(TableReader).FullName)
        {
        }

        public override int Depth => throw new NotImplementedException();

        public override bool IsClosed => throw new NotImplementedException();

        public override int RecordsAffected => throw new NotImplementedException();

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public async Task<IDataReader> Configure(TableQuery options, CancellationToken token)
        {
            var provider = new PagedTransport<,>();
            provider.Initialize(options.Transport);
            var stream = await options.Transport.GetStreamAsync(token);
            Json.Extract()
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            throw new NotImplementedException();
        }
    }

    public class TableQuery : ITransportable<ITransport<HttpContent>>, IConfigurableAsync<ITransport<HttpContent>, TableQuery>
    {
        public string AccountName;
        public string TableName;

        public ITransport<HttpContent> Transport { get; private set; }

        public Task<TableQuery> Configure(ITransport<HttpContent> options, CancellationToken token)
        {
            return Task.FromResult(new TableQuery { Transport = options, AccountName = this.AccountName, TableName = this.TableName });
        }
    }
}