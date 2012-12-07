using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Naive;
using Xunit;

namespace Naive.Tests
{
    public class NaiveDbTests : IDisposable
    {
        private readonly DirectoryInfo directory;
        private INaiveDb<TestClass> db;

        public NaiveDbTests()
        {
            this.directory = Directory.CreateDirectory(@"c:\dump\naive\test");
            this.db = NaiveDb.New<TestClass>(directory);
        }

        [Fact]
        public void Should()
        {
           Assert.True(File.Exists(@"c:\dump\naive\test\naive.db"));
        }

        public void Dispose()
        {
            db.Close();
            directory.Delete(true);
        }
    }

    internal class TestClass
    {
    }
}
