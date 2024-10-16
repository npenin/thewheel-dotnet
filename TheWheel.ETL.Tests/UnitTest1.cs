using System;
using System.Collections.Generic;
using System.DirectoryServices.Protocols;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Fluent;
using TheWheel.ETL.Provider.Ldap;
using TheWheel.ETL.Providers;
using TheWheel.Domain;
using System.Linq;
using System.Threading;

namespace TheWheel.ETL.Tests
{
    [TestClass]
    public class UnitTest1
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        public async Task TestLdapProvider()
        {
            var token = CancellationToken.None;
            var ldap = await Ldap.From("ldap.forumsys.com", token, new System.Net.NetworkCredential("cn=read-only-admin,dc=example,dc=com", "password"), AuthType.Basic)
            // .Query(new LdapOptions { Request = new SearchRequest("ou=scientists,dc=example,dc=com", null, SearchScope.Base), PageSize = 2 }, token);
            .Query(new LdapOptions { Request = new SearchRequest("dc=example,dc=com", "(&(objectClass=person)(uid=*))", SearchScope.Subtree), PageSize = 2 }, token);
            using (var reader = await ldap.ExecuteReaderAsync(token))
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(5, reader.FieldCount);
                for (var i = 0; i < reader.FieldCount; i++)
                    Console.WriteLine(reader.GetName(i));
                Assert.IsTrue(new[] { "inetOrgPerson", "organizationalPerson", "person", "top" }.SequenceEqual((string[])reader.GetValue("objectclass")));
                Assert.AreEqual("Newton", reader.GetString("sn"));
                Assert.AreEqual("Isaac Newton", reader.GetString("cn"));
                Assert.AreEqual("sn", reader.GetName("sn"));
                Assert.AreEqual("newton", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("einstein", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("tesla", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("galieleo", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("euler", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("gauss", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("riemann", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("euclid", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("test", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("curie", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("nobel", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("boyle", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("pasteur", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("nogroup", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("training", reader.GetString("uid"));
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("jmacy", reader.GetString("uid"));
                Assert.IsFalse(reader.Read());
            }
        }

        [TestMethod]
        public async Task TestCsvProvider()
        {
            var csv = await Helper
            .FromCsv<FileRead>("../../../test.csv", TestContext.CancellationTokenSource.Token)
            .Query(new CsvOptions { SkipLines = new string[4] });
            using (var reader = await csv.ExecuteReaderAsync(TestContext.CancellationTokenSource.Token))
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.FieldCount);
                Assert.AreEqual("column1", reader.GetName(0));
                Assert.AreEqual("column2", reader.GetName(1));
                Assert.AreEqual(reader.GetString(0), "dasdas,asdas");
                Assert.AreEqual(reader.GetString(1), "asdasa\nasdad\nasasdada\nd");
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(reader.GetString(0), @"sdadasd""asdasdas");
                Assert.AreEqual(reader.GetString(1), "asdasdads");
                Assert.IsFalse(reader.Read());
            }
        }


        [TestMethod]
        public async Task TestXmlProvider()
        {
            var xml = await Helper
            .FromXml<FileRead>("../../../../allitems-cvrf-year-2020.xml", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions().AddMatch("xml:///cvrfdoc/Vulnerability", "Title/text()", "CVE/text()"), TestContext.CancellationTokenSource.Token);
            int n = 1;
            using (var reader = await xml.ExecuteReaderAsync(TestContext.CancellationTokenSource.Token))
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.FieldCount);
                Assert.AreEqual("xml:///cvrfdoc/Vulnerability/Title/text()", reader.GetName(0));
                Assert.AreEqual("xml:///cvrfdoc/Vulnerability/CVE/text()", reader.GetName(1));
                while (reader.Read())
                    n++;
                Assert.AreEqual(33199, n);
            }
        }

        [TestMethod]
        public async Task TestXmlToCsv()
        {
            await Xml
            .From<FileRead>("../../../../allitems-cvrf-year-2020.xml", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } }, TestContext.CancellationTokenSource.Token)
            .Rename(new Dictionary<string, string> { { "xml:///cvrfdoc/Vulnerability/Title/text()", "Title" }, { "xml:///cvrfdoc/Vulnerability/CVE/text()", "CVE" } }, TestContext.CancellationTokenSource.Token)
            .To(Csv.To<FileWrite>("../../../testcveoutput.csv", TestContext.CancellationTokenSource.Token), new CsvReceiverOptions { Separator = Separator.Colon }, TestContext.CancellationTokenSource.Token);
        }


        [TestMethod]
        public async Task TestIfFlow()
        {
            await Xml.From<FileRead>("../../../../allitems-cvrf-year-2020.xml", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } })
            .If(reader => reader.GetString(reader.GetOrdinal("xml:///cvrfdoc/Vulnerability/Title/text()")) == reader.GetString(reader.GetOrdinal("xml:///cvrfdoc/Vulnerability/CVE/text()")), TestContext.CancellationTokenSource.Token)
            .Then(Csv.To<FileWrite>("../../../testcveoutput.csv", TestContext.CancellationTokenSource.Token),
                new CsvReceiverOptions { Separator = Separator.Colon }, TestContext.CancellationTokenSource.Token);
        }

        [TestMethod]
        public async Task TestLookup()
        {
            await Xml.From<FileRead>("../../../../allitems-cvrf-year-2020.xml", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } })
            .Lookup(Csv.From<FileRead>("../../../testcveoutput.csv", TestContext.CancellationTokenSource.Token).Query(new CsvOptions(), TestContext.CancellationTokenSource.Token).Cache(TestContext.CancellationTokenSource.Token), record => record.GetString(record.GetOrdinal("CVE/text()")), TestContext.CancellationTokenSource.Token)
            .WhenMatches(
                Csv.To<FileWrite>("../../../testcveoutput1.csv", TestContext.CancellationTokenSource.Token),
                new CsvReceiverOptions { Separator = Separator.Colon }, TestContext.CancellationTokenSource.Token)
                .WhenNotMatches(
                Csv.To<FileWrite>("../../../testcveoutput2.csv", TestContext.CancellationTokenSource.Token),
                new CsvReceiverOptions { Separator = Separator.Colon }, TestContext.CancellationTokenSource.Token);
        }

        [TestMethod]
        public async Task TestJson()
        {
            var json = await Json
            .From<FileRead>("../../../test.json", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } }, TestContext.CancellationTokenSource.Token);
            using (var reader = await json.ExecuteReaderAsync(TestContext.CancellationTokenSource.Token))
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.FieldCount);
                Assert.AreEqual("json:///results//column1/text()", reader.GetName(0));
                Assert.AreEqual("json:///results//column2/text()", reader.GetName(1));
                Assert.AreEqual(reader.GetString(0), "dasdas,asdas");
                Assert.AreEqual(reader.GetString(1), "asdasa\nasdad\nasasdada\nd");
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(reader.GetString(0), @"sdadasd""asdasdas");
                Assert.AreEqual(reader.GetString(1), "asdasdads");
                Assert.IsFalse(reader.Read());
            }
        }

        [TestMethod]
        public async Task TestCsvReceiver()
        {
            await Csv.To<FileWrite>("../../../testOutput.csv", TestContext.CancellationTokenSource.Token)
            .Receive(new CsvReceiverOptions { SkipLines = new string[] { "my first line header", "my second line header", "" }, Separator = Separator.Colon },
             await Json
             .From<FileRead>("../../../test.json", TestContext.CancellationTokenSource.Token)
            .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } })
            .Rename(new Dictionary<string, string> { { "json:///results//column1/text()", "column1" }, { "json:///results//column2/text()", "column3" } }));
        }


        [TestMethod]
        public async Task TestJsonReceiver()
        {
            await Json.To<FileWrite>("../../../testOutput.json", TestContext.CancellationTokenSource.Token)
            .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
                await Json
                .From<FileRead>("../../../test.json", TestContext.CancellationTokenSource.Token)
                .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } }, TestContext.CancellationTokenSource.Token));
        }

        [TestMethod]
        public async Task TestCsvReceiver2()
        {
            var csvHeader = new string[4];
            await Csv.To<FileWrite>("../../../testOutput.csv", TestContext.CancellationTokenSource.Token)
            .Receive(new CsvReceiverOptions { SkipLines = csvHeader, Separator = Separator.Colon },
                await Csv
                .From<FileRead>("../../../test.csv", TestContext.CancellationTokenSource.Token)
                .Query(new CsvOptions { SkipLines = csvHeader }, TestContext.CancellationTokenSource.Token));

            Assert.AreEqual(new System.IO.FileInfo("../../../test.csv").Length, new System.IO.FileInfo("../../../testOutput.csv").Length - 1);
        }

        [TestMethod]
        public async Task TestPaged()
        {
            var json = await Json.From(new PagedTransport<Http, Stream>("offset", "limit"), TestContext.CancellationTokenSource.Token, "https://neurovault.org/api/nidm_results/", new("limit", 100), new("offset", 0), new("_Content-Type", "application/json; utf-8"));
            await json.QueryAsync(new TreeOptions { TotalPath = "json:///count/text()" }.AddMatch("json:///results/", "id/text()", "name/text()").AddMatch("json:///count/text()"), TestContext.CancellationTokenSource.Token);
            await Csv.To<FileWrite>("../../../testOutput.csv", TestContext.CancellationTokenSource.Token)
            .Receive(new CsvReceiverOptions { Separator = Separator.Colon },
                json);
        }

        [TestMethod]
        public async Task TestJsonReceiver2()
        {
            await Json.To<FileWrite>("../../../testOutput.json", TestContext.CancellationTokenSource.Token)
            .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
            await Csv.From<FileRead>("../../../test.csv", TestContext.CancellationTokenSource.Token)
            .Query(new CsvOptions { SkipLines = new string[4] })
            .Rename(new Dictionary<string, string> { { "column1", "json:///results//column1/text()" }, { "column2", "json:///results//column2/text()" } })
            );
        }
        [TestMethod]
        public async Task TestJsonReceiver3()
        {
            await Json.To<FileWrite>("../../../testOutput.json", TestContext.CancellationTokenSource.Token)
           .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
           await Csv.From<FileRead>("../../../test.csv", TestContext.CancellationTokenSource.Token)
           .Query(new CsvOptions { SkipLines = new string[4] })
           );
        }

        [TestMethod]
        public async Task TestSimpleTransformation()
        {
            var csvHeader = new string[4];
            await Csv.To<FileWrite>("../../../testOutput.csv", TestContext.CancellationTokenSource.Token)
            .Receive(
                new CsvReceiverOptions { SkipLines = csvHeader, Separator = Separator.Colon },
                await Csv.From<FileRead>("../../../test.csv", TestContext.CancellationTokenSource.Token)
                .Query(new CsvOptions { SkipLines = csvHeader })
                .Transform("column1", (valueGetter) =>
                {
                    return valueGetter() + " pwic";
                }, TestContext.CancellationTokenSource.Token)
            );

            Assert.AreEqual(new System.IO.FileInfo("../../../test.csv").Length + " pwic pwic".Length, new System.IO.FileInfo("../../../testOutput.csv").Length - 1);
        }
    }
}
