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
        [JsonProperty("iTotalRecords")]
        public int TotalCount { get; set; }
        [JsonProperty("iTotalDisplayRecords")]
        public int Count { get; set; }
        [JsonProperty("sEcho")]
        public string Echo { get; set; }
        [JsonProperty("aaData")]
        public IEnumerable Data { get; set; }
    }

    public class DataTableQuery
    {
        [JsonProperty("iDisplayStart")]
        public int StartRowIndex { get; set; }
        [JsonProperty("iDisplayLength")]
        public int MaximumRows { get; set; }
        [JsonProperty("iColumns")]
        public int Columns { get; set; }
        [JsonProperty("bSearch")]
        public string Search { get; set; }
        [JsonProperty("bRegex")]
        public bool Regex { get; set; }
        [JsonProperty("iSortingCols")]
        public int SortingCols { get; set; }
        [JsonProperty("sEcho")]
        public int Echo { get; set; }
    }
}
