using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TheWheel.Lambda;
using TheWheel.Domain;
using TheWheel.Services;
using System.Linq;
using System.Linq.Expressions;
using System.IO;

namespace TheWheel.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestPartiaEval()
        {
            var it = new { Property = "pwic" };
            Expression<Func<FilterCriteria, bool>> filter = f => f.PropertyName == it.Property;
            Expression filterReduced = filter.PartialEval();
            Assert.AreEqual(ExpressionType.Constant, ((BinaryExpression)((LambdaExpression)filterReduced).Body).Right.NodeType);
        }

        [TestMethod]
        public void TestPropertyAccessor()
        {
            var it = new { Property = "pwic" };
            Assert.AreEqual("pwic", it.Property("Property"));
        }

        [TestMethod]
        public void TestExcelExport()
        {
            TheWheel.OpenXml.Extensions.Export(File.Create("test.xlsx"), Enumerable.Range(0, 10).Select(i => new
            {
                a = i,
                b = i,
                c = i,
                d = i,
                e = i,
                f = i,
                g = i,
                h = i,
                i = i,
                j = i,
                k = i,
                l = i,
                m = i,
                n = i,
                o = i,
                p = i,
                q = i,
                r = i,
                s = i,
                t = i,
                u = i,
                v = i,
                w = i,
                x = i,
                y = i,
                z = i,
                aa = i,
                ab = i,
            }), new[]{
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
                "q",
                "r",
                "s",
                "t",
                "u",
                "v",
                "w",
                "x",
                "y",
                "z",
                "aa",
                "ab"}.Select((c, i) => new Column { Code = c, Name = c.ToUpper(), TranslatedName = c.ToUpper(), Id = i+1 }), "Column");
        }

        [TestMethod]
        public void TestFilter()
        {
            var filter = new Filter
            {
                Name = "pwic",
                FilterCriterias ={
                    new FilterCriteria{
                        PropertyName="Property",
                        PropertyValue="pwic"
                    }
                }
            };
            var source = new[] {
                new {
                    Property = "pwic",
                    Children = new[] { new { Property1 = "pwic1",Property2 = "1pwic" }, new { Property1 = "pwic2",Property2 = "2pwic" }, new { Property1 = "pwic3",Property2 = "2pwic" } }
                },
                new {
                    Property = "pwet",
                    Children = new[]{new { Property1 = "pwet1",Property2 = "1pwet"  } }
                }
            };
            var fqb = FilterQueryBuilder.Create(source.AsQueryable());
            fqb.Visit(filter);

            Assert.AreEqual("pwic", fqb.Query.Single().Property);

            filter = new Filter
            {
                Name = "pwic",
                FilterCriterias ={
                    new FilterCriteria{
                        PropertyName="Children.Property1",
                        PropertyValue="pwic",
                        FilterOperator=FilterOperator.StartsWith,
                        FilterCriterias={
                            new FilterCriteria{
                                PropertyName=".Property2",
                                PropertyValue="2pwic",
                                FilterOperator=FilterOperator.Equal,
                            }
                        }
                    }
                }
            };

            fqb = FilterQueryBuilder.Create(source.AsQueryable());
            fqb.Visit(filter);

            Assert.AreEqual(source.Count(it => it.Children.Any(c => c.Property1.StartsWith("pwic") && c.Property2 == "2pwic")), fqb.Query.Count());
        }
    }
}
