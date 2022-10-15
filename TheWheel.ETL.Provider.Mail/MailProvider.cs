using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Provider.Mail
{
    public class MailDataProvider<MailTransport> : DataProvider<MailTransport>
    where MailTransport : ITransport, IDataReader
    {
        public override Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return Task.FromResult<IDataReader>(Transport);
        }
    }
}