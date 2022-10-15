using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public interface IConfigurable<TOptions, TThis>
    {
        TThis Configure(TOptions options);
    }
    public interface IConfigurableAsync<TOptions, TThis>
    {
        Task<TThis> Configure(TOptions options, CancellationToken token);
    }
}