using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Naive;
using Xunit;
using System.Threading;

namespace Naive.Tests
{
    public class NaiveDbTests : IDisposable
    {
        public NaiveDbTests()
        {
        }

        [Fact]
        public void ShouldCreateDatabaseFolderWithClassName()
        {
            DeleteDirectory(@"c:\dump\naive\testclass");

            var baseDirectory = new DirectoryInfo(@"c:\dump\naive");
           
            using (var db = NaiveDb.New<TestClass>(baseDirectory))
            {
                Assert.True(Directory.Exists(@"c:\dump\naive\testclass"));

                db.Close();
            }

            Directory.Delete(@"c:\dump\naive\testclass", true);
        }

        [Fact]
        public void ShouldCreateMemtableFileAfterFirstPut()
        {
            DeleteDirectory(@"c:\dump\naive\testput");

            var baseDirectory = new DirectoryInfo(@"c:\dump\naive");
            var key = Guid.NewGuid().ToString();

            using (var db = NaiveDb.New<TestPut>(baseDirectory))
            {
                var test1 = new TestPut { Name = "test1" };
                db.Put("key1", test1);
                db.Put("key2", new TestPut { Name = "test2" });
                db.Put("key3", new TestPut { Name = "test3" });
                db.Put("key4", new TestPut { Name = "test4" });

                Assert.True(Directory.Exists(@"c:\dump\naive\testput"), "testput directory does no exist");

                Assert.True(File.Exists(@"c:\dump\naive\testput\memtable.log"), "memtable.log does not exist");
                Thread.Sleep(100);

                var result1 = db.Get("key1");
                var result2 = db.Get("key2");
                var result3 = db.Get("key3");
                var result4 = db.Get("key4");

                Assert.Equal("test1", result1.Name);
                Assert.Equal("test2", result2.Name);
                Assert.Equal("test3", result3.Name);
                Assert.Equal("test4", result4.Name);

                db.Close();
            }

            DeleteDirectory(@"c:\dump\naive\testput");
        }

        [Fact]
        public void ShouldReadPersistedMemtableWhenReopened()
        {
            DeleteDirectory(@"c:\dump\naive\testput");

            var baseDirectory = new DirectoryInfo(@"c:\dump\naive");
            var key = Guid.NewGuid().ToString();

            using (var db = NaiveDb.New<TestPut>(baseDirectory))
            {
                var test1 = new TestPut { Name = "test1" };
                db.Put("key1", test1);
                db.Put("key2", new TestPut { Name = "test2" });
                db.Put("key3", new TestPut { Name = "test3" });
                db.Put("key4", new TestPut { Name = "test4" });

                Assert.True(Directory.Exists(@"c:\dump\naive\testput"), "testput directory does no exist");

                Assert.True(File.Exists(@"c:\dump\naive\testput\memtable.log"), "memtable.log does not exist");
                Thread.Sleep(100);

                db.Close();
            }

            using (var db = NaiveDb.New<TestPut>(baseDirectory))
            {
                db.Put("key5", new TestPut { Name = "test5" });

                var result1 = db.Get("key1");
                var result2 = db.Get("key2");
                var result3 = db.Get("key3");
                var result4 = db.Get("key4");
                var result5 = db.Get("key5");

                Assert.Equal("test1", result1.Name);
                Assert.Equal("test2", result2.Name);
                Assert.Equal("test3", result3.Name);
                Assert.Equal("test4", result4.Name);
                Assert.Equal("test5", result5.Name);

                db.Close();
            }

            DeleteDirectory(@"c:\dump\naive\testput");
        }
        private static void DeleteDirectory(string dir)
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, true);
            }
        }

        public void Dispose()
        {
        }
    }
    internal class TestPut
    {
        public string Name { get; set; }
    }

    internal class TestClass
    {
    }
}
