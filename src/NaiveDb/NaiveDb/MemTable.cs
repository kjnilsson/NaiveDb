using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NaiveDb
{
    internal class MemTable : IDisposable
    {
        private IDictionary<string, string> cache = new ConcurrentDictionary<string, string>();
        private IDictionary<string, long> index = new SortedDictionary<string, long>();
        
        private FileStream fs;
        private StreamWriter indexSw;
        private int current;

        public MemTable(string basePath)
        {
            var path = Path.Combine(basePath, "memtable.log");
            var indexPath = Path.Combine(basePath, "memtable.index");
            if (File.Exists(path))
            {
                File.ReadAllLines(indexPath).Select(s =>
                {
                    var bits = s.Split(':');
                    return new { Key = bits[0], Value = bits[1] };
                })
                .ToList()
                .ForEach(x => index.Add(x.Key, long.Parse(x.Value)));

                var bytes = File.ReadAllBytes(path);

                for (int i = 0; i < index.Count(); i++)
			    {
                    var item = index.ElementAt(i);
                    var next = index.ElementAtOrDefault(i + 1);
                    var length = default(KeyValuePair<string,long>).Equals(next) ? bytes.Length - item.Value : next.Value - item.Value;
                    cache.Add(item.Key, Encoding.UTF8.GetString(bytes, (int)item.Value, (int)length));
                }   

                this.fs = File.Open(path, FileMode.Append);
                this.indexSw = File.AppendText(indexPath);

                this.current = bytes.Length;
            }
            else
            {
                this.fs = File.Create(path);
                this.indexSw = File.CreateText(indexPath);
                this.current = 0;
            }
        }

        internal void Write(string key, string value)
        {
            cache.Add(key, value);

            var bytes = Encoding.UTF8.GetBytes(value);
            
            fs.Write(bytes, 0, bytes.Length);
            index.Add(key, current);
            indexSw.WriteLine(key + ':' + current.ToString());
            fs.Flush();
            current += bytes.Length;
        }

        public void Dispose()
        {
            this.indexSw.Close();
            this.indexSw.Dispose();
            this.fs.Close();
            this.fs.Dispose();
        }

        internal string Read(string key)
        {
            string value = null;
            cache.TryGetValue(key, out value);
            return value; // or throw?
        }
    }
}
