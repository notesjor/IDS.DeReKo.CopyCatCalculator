using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hyldahl.Hashing.SpamSum;
using Newtonsoft.Json;

namespace IDS.DeReKo.CopyCatCalculator
{
  class Program
  {
    static void Main(string[] args)
    {
      var baseDir = args[0];
      var searchFiles = Directory.GetFiles(baseDir, args[1]);

      var searchQueries = searchFiles.Select(ReadCorpusExplorerOutput)
                                     .SelectMany(tmp => tmp)
                                     .ToDictionary(x => x.Key, x => x.Value);

      var allFiles = Directory.GetFiles(baseDir, "*.tsv");
      var filter = new HashSet<string>(searchFiles);

      var giffeys = new Dictionary<Guid, Dictionary<string, Dictionary<Guid, int>>>();
      var @lock = new object();

      Parallel.ForEach(allFiles, file =>
      {
        if (filter.Contains(file))
          return;

        var content = ReadCorpusExplorerOutput(file);
        foreach (var c in content)
        {
          foreach (var s in searchQueries)
          {
            var size = FuzzyHashing.Compare(c.Value, s.Value);
            if (size < 95)
              continue;

            lock (@lock)
              if (giffeys.ContainsKey(s.Key))
              {
                if (giffeys[s.Key].ContainsKey(file))
                  giffeys[s.Key][file].Add(c.Key, size);
                else
                  giffeys[s.Key].Add(file, new Dictionary<Guid, int>
                {
                  {c.Key, size}
                });
              }
              else
              {
                giffeys.Add(s.Key, new Dictionary<string, Dictionary<Guid, int>>
              {
                {
                  file, new Dictionary<Guid, int>
                  {
                    {c.Key, size}
                  }
                }
              });
              }
          }
        }
      });

      File.WriteAllText(Path.Combine(baseDir + ".json"), JsonConvert.SerializeObject(giffeys), Encoding.UTF8);
    }

    private static Dictionary<Guid, SpamSumSignature> ReadCorpusExplorerOutput(string path)
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
