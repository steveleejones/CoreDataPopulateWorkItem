using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testbed
{
    class Program
    {
        static void Main(string[] args)
        {
            CoreDataPopulateWorkItem.CoreDataPopulateWorkItem workItem = new CoreDataPopulateWorkItem.CoreDataPopulateWorkItem();
            workItem.Run();
        }
    }
}
