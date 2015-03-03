using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TheWheel.Lambda;
using TheWheel.Domain;
using TheWheel.Services;
using System.Linq;

namespace TheWheel.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestPropertyAccessor()
        {
            var it = new { Property = "pwic" };
            Assert.AreEqual("pwic", it.Property("Property"));
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
