using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Dto
{
    public class DataTableView
    {
        public int TotalCount { get; set; }
        public int Count { get; set; }
        public IEnumerable Data { get; set; }
        public int Echo { get; set; }
    }

    public class DataTableView<T>
    {
        public int TotalCount { get; set; }
        public int Count { get; set; }
        public IEnumerable<T> Data { get; set; }
        public int Echo { get; set; }
    }

    public class DataTableQuery
    {
        public int StartRowIndex { get; set; }
        public int MaximumRows { get; set; }
        public int Columns { get; set; }
        public string Search { get; set; }
        public bool Regex { get; set; }
        public int SortingCols { get; set; }
    }
}
