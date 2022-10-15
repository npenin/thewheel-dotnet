using System;
using System.Data;
using System.DirectoryServices.Protocols;
using System.Threading.Tasks;
using System.Linq;
using TheWheel.ETL.Contracts;
using System.Threading;

namespace TheWheel.ETL.Provider.Ldap
{
    public class LdapReader : DataReader, IConfigurableAsync<LdapOptions, IDataReader>
    {
        private bool isClosed;
        private LdapOptions options;
        private SearchResponse response;
        private int index = -1;

        public LdapReader() : base("TheWheel.ETL.Ldap")
        {
        }

        public LdapReader(SearchResponse response) : this()
        {
            this.response = response;
        }

        public override int Depth => 0;

        public override bool IsClosed => isClosed;

        public override int RecordsAffected => throw new NotImplementedException();

        public override void Close()
        {
            isClosed = true;
        }

        public async Task<IDataReader> Configure(LdapOptions options, CancellationToken token)
        {
            var connection = await options.Transport.GetStreamAsync(token);
            SearchResponse response;
            if (options.Timeout.HasValue)
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, options.Timeout.Value, System.DirectoryServices.Protocols.PartialResultProcessing.NoPartialResultSupport, callback, state),
                connection.EndSendRequest, null);
            else
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, System.DirectoryServices.Protocols.PartialResultProcessing.NoPartialResultSupport, callback, state),
               connection.EndSendRequest, null);
            if (response.ResultCode == ResultCode.Success)
                return new LdapReader(response) { options = options };
            throw new LdapException((int)response.ResultCode, response.ErrorMessage);
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
            if (response == null)
                return false;
            if (response.Entries.Count >= ++index)
                return false;
            var current = response.Entries[index];
            Current = new DataRecord(current.Attributes.Values.Cast<object>().ToArray(), current.Attributes.AttributeNames.Cast<string>().ToArray());
            return true;
        }
    }
}