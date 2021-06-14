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
      var searchFiles = Directory.GetFiles(baseDir, args[2]);

      var searchQueries = searchFiles.Select(ReadCorpusExplorerOutput)
                                     .SelectMany(tmp => tmp)
                                     .ToDictionary(x => x.Key, x => x.Value);

      var allFiles = Directory.GetFiles(baseDir, "*.tsv");
      var filter = new HashSet<string>(searchFiles);

      var @lock = new object();
      using (var fs = new FileStream(args[3], FileMode.Create, FileAccess.Write))
      using (var bf = new BufferedStream(fs, 4096))
      using (var writer = new StreamWriter(bf))
        Parallel.ForEach(allFiles, file =>
        {
          if (filter.Contains(file))
            return;

          Console.WriteLine($">>> {file}");

          var content = ReadCorpusExplorerOutput(file);
          foreach (var c in content)
          {
            foreach (var s in searchQueries)
            {
              var size = FuzzyHashing.Compare(c.Value, s.Value);
              if (size < minimum)
                continue;

              lock (@lock)
                writer.WriteLine(string.Join("\t", 
                                             s.Key.ToString("N"), 
                                             size.ToString(),
                                             file, 
                                             c.Key));
            }
          }

          Console.WriteLine($"<<< {file}");
        });
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
