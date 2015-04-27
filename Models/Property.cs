using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreDataPopulateWorkItem.Models
{
    internal class Property
    {
        public int PropertyReferenceId { get; set; }
        //public int TTIId { get; set; }
        public string PropertyName { get; set; }
        public string Country { get; set; }
        public string Region { get; set; }
        public string Resort { get; set; }
        public int CountryId { get; set; }
        public int RegionId { get; set; }
        public int ResortId { get; set; }
        public int IncludesOwnStock { get; set; }
        public int BestSeller { get; set; }
        public int Rating { get; set; }
        public int ContractCount { get; set; }
        public int PropertyTypeId { get; set; }
        public string CountryCode { get; set; }
    }
}
