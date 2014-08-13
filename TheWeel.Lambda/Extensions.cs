using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Lambda
{
    public static class Extensions
    {
        public static int Count(this IEnumerable source)
        {
            var enumerator = source.GetEnumerator();
            var n = 0;
            while (enumerator.MoveNext())
                n++;
            return n;
        }
    }
}
