using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hyldahl.Hashing.SpamSum;

namespace IDS.DeReKo.CopyCatCalculator
{
  class Program
  {
    static void Main(string[] args)
    {
      var baseDir = args[0];
      var minimum = int.Parse(args[1]);
      var allFiles = Directory.GetFiles(baseDir, "*.tsv");

      var lock1 = new object();
      var model = new Dictionary<string, Dictionary<Guid, SpamSumSignature>>();
      Console.Write("Load Model...");
      Parallel.ForEach(allFiles, file =>
      {
        var entry = ReadCorpusExplorerModelEntry(file);
        lock (lock1)
          model.Add(Path.GetFileName(file), entry);
      });
      Console.WriteLine("ok!");

      var filter = new HashSet<string>(Directory.GetFiles(baseDir, args[2]).Select(Path.GetFileName));

      var lock2 = new object();
      var cnt = 0;
      var max = allFiles.Length * filter.Count;

      using (var fs = new FileStream(args[3], FileMode.Create, FileAccess.Write, FileShare.Read))
      using (var bf = new BufferedStream(fs, 4096))
      using (var writer = new StreamWriter(bf, Encoding.UTF8))
        Parallel.ForEach(filter, new ParallelOptions { MaxDegreeOfParallelism = 2 }, searchFile =>
          {
            var searchQueries = model[searchFile];

            Parallel.ForEach(allFiles, new ParallelOptions { MaxDegreeOfParallelism = 4 }, file =>
            {
              lock (lock2)
                cnt++;

              file = Path.GetFileName(file);
              if (filter.Contains(file))
                return;

              Console.WriteLine($">>> {file}");

              var content = model[file];
              var cache = new List<string>();
              var lock3 = new object();

              Parallel.ForEach(searchQueries, new ParallelOptions { MaxDegreeOfParallelism = 10 }, s =>
              {
                foreach (var c in content)
                {
                  var size = FuzzyHashing.Compare(c.Value, s.Value);
                  if (size < minimum)
                    continue;

                  lock (lock3)
                    cache.Add(string.Join("\t", Path.GetFileName(searchFile), s.Key.ToString("N"), size.ToString(), Path.GetFileName(file), c.Key.ToString("N")));
                }
              });

              lock (lock1)
                foreach (var line in cache)
                  writer.WriteLine(line);

              lock (lock2)
                Console.WriteLine($"<<< [{cnt} / {max}] {file} (CopyCats: {cache.Count})");
            });
          });
    }

    private static Dictionary<Guid, SpamSumSignature> ReadCorpusExplorerModelEntry(string path)
    {
      var res = new Dictionary<Guid, SpamSumSignature>();

      using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
      using (var reader = new StreamReader(fs, Encoding.UTF8))
      {
        reader.ReadLine();
        while (!reader.EndOfStream)
        {
          var cells = reader.ReadLine()?.Split(new[] { "\t" }, StringSplitOptions.RemoveEmptyEntries);
          if (cells.Length != 2)
            continue;
          res.Add(Guid.Parse(cells[0]), new SpamSumSignature(cells[1]));
        }
      }

      return res;
    }
  }
}
