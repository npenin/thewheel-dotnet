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
using System.Threading;

namespace TheWheel.ETL.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public async Task TestCsvProvider(CancellationToken token)
        {
            var csv = await Helper
            .FromCsv<FileRead>("../../../test.csv", token)
            .Query(new CsvOptions { SkipLines = new string[4] });
            using (var reader = await csv.ExecuteReaderAsync(token))
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
        public async Task TestXmlProvider(CancellationToken token)
        {
            var xml = await Helper
            .FromXml<FileRead>("../../../../allitems-cvrf-year-2020.xml", token)
            .Query(new TreeOptions().AddMatch("xml:///cvrfdoc/Vulnerability", "Title/text()", "CVE/text()"), token);
            int n = 1;
            using (var reader = await xml.ExecuteReaderAsync(token))
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.FieldCount);
                Assert.AreEqual("xml:///cvrfdoc/Vulnerability/Title/text()", reader.GetName(0));
                Assert.AreEqual("xml:///cvrfdoc/Vulnerability/CVE/text()", reader.GetName(1));
                while (reader.Read())
                    n++;
                Assert.AreEqual(31114, n);
            }
        }

        [TestMethod]
        public async Task TestXmlToCsv(CancellationToken token)
        {
            await Xml
            .From<FileRead>("../../../../allitems-cvrf-year-2020.xml", token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } }, token)
            .Rename(new Dictionary<string, string> { { "xml:///cvrfdoc/Vulnerability/Title/text()", "Title" }, { "xml:///cvrfdoc/Vulnerability/CVE/text()", "CVE" } }, token)
            .To(Csv.To<FileWrite>("../../../testcveoutput.csv", token), new CsvReceiverOptions { Separator = Separator.Colon }, token);
        }


        [TestMethod]
        public async Task TestIfFlow(CancellationToken token)
        {
            await Xml.From<FileRead>("../../../../allitems-cvrf-year-2020.xml", token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } })
            .If(reader => reader.GetString(reader.GetOrdinal("xml:///cvrfdoc/Vulnerability/Title/text()")) == reader.GetString(reader.GetOrdinal("xml:///cvrfdoc/Vulnerability/CVE/text()")), token)
            .Then(Csv.To<FileWrite>("../../../testcveoutput.csv", token),
                new CsvReceiverOptions { Separator = Separator.Colon }, token);
        }

        [TestMethod]
        public async Task TestLookup(CancellationToken token)
        {
            await Xml.From<FileRead>("../../../../allitems-cvrf-year-2020.xml", token)
            .Query(new TreeOptions { Root = "xml:///cvrfdoc/Vulnerability", Paths = new TreeLeaf[] { "Title/text()", "CVE/text()" } })
            .Lookup(Csv.From<FileRead>("../../../testcveoutput.csv", token).Query(new CsvOptions(), token).Cache(token), record => record.GetString(record.GetOrdinal("CVE/text()")), token)
            .WhenMatches(
                Csv.To<FileWrite>("../../../testcveoutput1.csv", token),
                new CsvReceiverOptions { Separator = Separator.Colon }, token)
                .WhenNotMatches(
                Csv.To<FileWrite>("../../../testcveoutput2.csv", token),
                new CsvReceiverOptions { Separator = Separator.Colon }, token);
        }

        [TestMethod]
        public async Task TestJson(CancellationToken token)
        {
            var json = await Json
            .From<FileRead>("../../../test.json", token)
            .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } }, token);
            using (var reader = await json.ExecuteReaderAsync(token))
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
        public async Task TestCsvReceiver(CancellationToken token)
        {
            await Csv.To<FileWrite>("../../../testOutput.csv", token)
            .Receive(new CsvReceiverOptions { SkipLines = new string[] { "my first line header", "my second line header", "" }, Separator = Separator.Colon },
             await Json
             .From<FileRead>("../../../test.json", token)
            .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } })
            .Rename(new Dictionary<string, string> { { "json:///results//column1/text()", "column1" }, { "json:///results//column2/text()", "column3" } }));
        }


        [TestMethod]
        public async Task TestJsonReceiver(CancellationToken token)
        {
            await Json.To<FileWrite>("../../../testOutput.json", token)
            .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
                await Json
                .From<FileRead>("../../../test.json", token)
                .Query(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } }, token));
        }

        [TestMethod]
        public async Task TestCsvReceiver2(CancellationToken token)
        {
            var csvHeader = new string[4];
            await Csv.To<FileWrite>("../../../testOutput.csv", token)
            .Receive(new CsvReceiverOptions { SkipLines = csvHeader, Separator = Separator.Colon },
                await Csv
                .From<FileRead>("../../../test.csv", token)
                .Query(new CsvOptions { SkipLines = csvHeader }, token));

            Assert.AreEqual(new System.IO.FileInfo("../../../test.csv").Length, new System.IO.FileInfo("../../../testOutput.csv").Length - 1);
        }

        [TestMethod]
        public async Task TestPaged(CancellationToken token)
        {
            var json = await Json.From(new PagedTransport<Http, Stream>("offset", "limit"), token, "https://neurovault.org/api/nidm_results/", new("limit", 100), new("offset", 0), new("_Content-Type", "application/json; utf-8"));
            await json.QueryAsync(new TreeOptions { TotalPath = "json:///count/text()" }.AddMatch("json:///results/", "id/text()", "name/text()").AddMatch("json:///count/text()"), token);
            await Csv.To<FileWrite>("../../../testOutput.csv", token)
            .Receive(new CsvReceiverOptions { Separator = Separator.Colon },
                json);
        }

        [TestMethod]
        public async Task TestJsonReceiver2(CancellationToken token)
        {
            await Json.To<FileWrite>("../../../testOutput.json", token)
            .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
            await Csv.From<FileRead>("../../../test.csv", token)
            .Query(new CsvOptions { SkipLines = new string[4] })
            .Rename(new Dictionary<string, string> { { "column1", "json:///results//column1/text()" }, { "column2", "json:///results//column2/text()" } })
            );
        }
        [TestMethod]
        public async Task TestJsonReceiver3(CancellationToken token)
        {
            await Json.To<FileWrite>("../../../testOutput.json", token)
           .Receive(new TreeOptions { Root = "json:///results/", Paths = new TreeLeaf[] { "column1/text()", "column2/text()" } },
           await Csv.From<FileRead>("../../../test.csv", token)
           .Query(new CsvOptions { SkipLines = new string[4] })
           );
        }

        [TestMethod]
        public async Task TestSimpleTransformation(CancellationToken token)
        {
            var csvHeader = new string[4];
            await Csv.To<FileWrite>("../../../testOutput.csv", token)
            .Receive(
                new CsvReceiverOptions { SkipLines = csvHeader, Separator = Separator.Colon },
                await Csv.From<FileRead>("../../../test.csv", token)
                .Query(new CsvOptions { SkipLines = csvHeader })
                .Transform("column1", (valueGetter) =>
                {
                    return valueGetter() + " pwic";
                }, token)
            );

            Assert.AreEqual(new System.IO.FileInfo("../../../test.csv").Length + " pwic pwic".Length, new System.IO.FileInfo("../../../testOutput.csv").Length - 1);
        }
    }
}
