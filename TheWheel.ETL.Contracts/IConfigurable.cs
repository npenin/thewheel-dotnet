namespace TheWheel.ETL.Contracts
{
    public interface IConfigurable<TOptions, TThis>
    {
        TThis Configure(TOptions options);
    }
}