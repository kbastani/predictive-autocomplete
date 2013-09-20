using PredictiveAutocomplete;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PredictiveAutocomplete_Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            List<IGraphNode> results = Processor.GetRankedNodesForQuery("topic", "phrase:(\\\"MATHEMATICS\\\")", string.Empty, "Key", 0, 20).OrderByDescending(g => g.size).ToList();

            Processor.IndexAutoCompleteKey(results, Configuration.GetDataConnectionString(), Configuration.GetAutoCompleteCacheId(), 10, 40);

            foreach (var item in results)
            {
                Console.WriteLine(string.Format("{0} : {1}", item.label, item.size));
            }

            Console.ReadLine();
        }

    }
}
