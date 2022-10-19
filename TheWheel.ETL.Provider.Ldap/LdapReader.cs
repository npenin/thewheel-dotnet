using System;
using System.Data;
using System.DirectoryServices.Protocols;
using System.Threading.Tasks;
using System.Linq;
using TheWheel.ETL.Contracts;
using System.Threading;

namespace TheWheel.ETL.Provider.Ldap
{
    public class LdapReader : DataReader, IConfigurableAsync<LdapOptions, IDataReader>, IPageable
    {
        private bool isClosed;
        private LdapOptions options;
        private SearchResponse response;
        private int index = -1;
        private LdapConnection connection;
        private int totalCount;
        private PageResultRequestControl pageRequestControl;

        public LdapReader() : base("TheWheel.ETL.Ldap")
        {
        }

        public LdapReader(SearchResponse response) : this()
        {
            this.response = response;
            processed = response.Entries.Count;
        }

        private int processed = 0;
        public override int Depth => 0;

        public override bool IsClosed => isClosed;

        public override int RecordsAffected => totalCount;

        public int Total { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Close()
        {
            isClosed = true;
        }

        public async Task<IDataReader> Configure(LdapOptions options, CancellationToken token)
        {
            pageRequestControl = new PageResultRequestControl(options.PageSize ?? 1000);

            // used to retrieve the cookie to send for the subsequent request
            options.Request.Controls.Add(pageRequestControl);

            connection = await options.Transport.GetStreamAsync(token);
            SearchResponse response;
            if (options.Timeout.HasValue)
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, options.Timeout.Value, System.DirectoryServices.Protocols.PartialResultProcessing.NoPartialResultSupport, callback, state),
                connection.EndSendRequest, null);
            else
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, System.DirectoryServices.Protocols.PartialResultProcessing.NoPartialResultSupport, callback, state),
               connection.EndSendRequest, null);
            if (response.ResultCode == ResultCode.Success)
            {
                totalCount = response.Controls.OfType<PageResultResponseControl>().First().TotalCount;
                return new LdapReader(response) { options = options };
            }
            throw new LdapException((int)response.ResultCode, response.ErrorMessage);
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public async Task NextPage(CancellationToken token)
        {
            if (processed >= totalCount)
            {
                await Task.FromCanceled(token);
            }
            PageResultResponseControl pageResponseControl = response.Controls.OfType<PageResultResponseControl>().First();

            PageResultRequestControl pageRequestControl = new PageResultRequestControl(pageResponseControl.Cookie);

            // used to retrieve the cookie to send for the subsequent request
            options.Request.Controls[options.Request.Controls.IndexOf(this.pageRequestControl)] = pageRequestControl;
            this.pageRequestControl = pageRequestControl;

            if (options.Timeout.HasValue)
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, options.Timeout.Value, System.DirectoryServices.Protocols.PartialResultProcessing.ReturnPartialResultsAndNotifyCallback, callback, state),
                connection.EndSendRequest, null);
            else
                response = (SearchResponse)await Task.Factory.FromAsync((callback, state) => connection.BeginSendRequest(options.Request, System.DirectoryServices.Protocols.PartialResultProcessing.ReturnPartialResultsAndNotifyCallback, callback, state),
               connection.EndSendRequest, null);
            if (response.ResultCode == ResultCode.Success)
            {
                var totalCount = response.Controls.OfType<PageResultResponseControl>().First().TotalCount;
                if (!options.IgnoreCountCheck && totalCount != this.totalCount)
                    throw new LdapException("The total count has changed in between. Please consider discarding your results. If you are not willing to receive this error message, please consider setting " + nameof(options.IgnoreCountCheck) + " to true.");
                processed += response.Entries.Count;
                return;
            }
            throw new LdapException((int)response.ResultCode, response.ErrorMessage);
        }

        public override bool NextResult()
        {
            NextPage(CancellationToken.None).ContinueWith(t =>
            {
                if (t.Status == TaskStatus.Canceled)
                    response = null;
            }).Wait();
            return Read();
        }

        public override bool Read()
        {
            Console.WriteLine(processed);
            if (response == null)
                return false;
            index++;
            Console.WriteLine(index);
            if (index >= response.Entries.Count)
                return false;
            var current = response.Entries[index];
            Current = new DataRecord(current.Attributes.Values.Cast<DirectoryAttribute>().Select(Format).ToArray(), current.Attributes.AttributeNames.Cast<string>().ToArray());
            return true;
        }

        public object Format(DirectoryAttribute attribute)
        {
            var values = new object[attribute.Count];
            // Console.WriteLine(attribute.Name);
            switch (attribute.Name)
            {
                case "objectClass":
                case "uniqueMember":
                    attribute.CopyTo(values, 0);
                    var result = values.Select(v => System.Text.Encoding.UTF8.GetString((byte[])v)).ToArray();
                    // Console.WriteLine(string.Join(',', result));
                    return result;
                case "cn":
                case "ou":
                default:
                    if (attribute.Count == 1)
                    {
                        // Console.WriteLine(string.Join(',', attribute[0]));
                        return attribute[0];
                    }
                    attribute.CopyTo(values, 0);
                    // Console.WriteLine(string.Join(',', values));
                    return values;
            }
        }
    }
}