namespace TheWheel.ETL.DacPac
{
    public class TableModel
    {
        public int object_id;
        public string type;
        public string name;
        public ColumnModel[] columns;
        public ParameterModel[] parameters;
    }
}