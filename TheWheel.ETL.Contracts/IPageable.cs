using System.Threading.Tasks;
using System.Threading;

namespace TheWheel.ETL.Contracts
{
    public interface IPageable
    {
        int Total { get; set; }

        Task NextPage(CancellationToken token);
    }

}
