using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TheWheel.Lambda;
using TheWheel.Domain;
using TheWheel.Services;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;

namespace TheWheel.Tests
{
    [TestClass]
    public class TestReflectionExpressions
    {
        private class A
        {
            public IEnumerable<B> Property { get; set; }
        }

        private class B
        {
            public int a { get; set; }
            public string b { get; set; }
        }

        [TestMethod]
        public void TestSelectMany()
        {
            var it = new A { Property = new[] { new B { a = 1, b = "pwic" } } };
            var them = new[] { it };
            Assert.AreEqual(them.SelectMany(x => x.Property, (x, p) => p.a).Count(x => x == 1),
                them.AsQueryable().SelectMany(typeof(A), typeof(B), typeof(int), (Expression<Func<A, IEnumerable<B>>>)(x => x.Property), (Expression<Func<A, B, int>>)((x, p) => p.a)).Cast<int>().Count(x => x == 1));
        }
    }
}
