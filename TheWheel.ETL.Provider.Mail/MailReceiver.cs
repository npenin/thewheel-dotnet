using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Provider.Mail
{
    public class MailReceiverOptions
    {

    }

    public class MailReceiver : IDataReceiver<MailReceiverOptions>
    {
        public async Task ReceiveAsync(IDataProvider provider, MailReceiverOptions query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {

            }
        }
    }

}