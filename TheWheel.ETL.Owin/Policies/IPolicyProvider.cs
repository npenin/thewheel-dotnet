using System.Collections.Generic;
using System.Threading.Tasks;
using TheWheel.ETL.DacPac;

namespace TheWheel.ETL.Owin
{
    public interface IPolicyProvider
    {
        // IEnumerable<TableModel> Allowed(IEnumerable<TableModel> model);
        Task<IEnumerable<TableModel>> AllowedAsync(IEnumerable<TableModel> model, OperationType operation = OperationType.Read);
        // bool IsAllowed(TableModel model);
        Task<bool> IsAllowedAsync(TableModel model, OperationType operation = OperationType.Read);
    }

    public enum OperationType
    {
        Create,
        Read,
        Update,
        Delete
    }
}