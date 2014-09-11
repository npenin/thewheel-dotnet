using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Dto
{
    public static class Extensions
    {
        public static DataTableView ToGridData<T>(IQueryable<T> source, int startRowIndex, int maximumRows)
        {
            return new DataTableView()
            {
                Count = source.Count(),
                Data = source.Skip(startRowIndex).Take(maximumRows)
            };
        }
    }
}
