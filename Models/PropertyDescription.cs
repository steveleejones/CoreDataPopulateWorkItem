using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreDataPopulateWorkItem.Models
{
    public class PropertyDescription
    {
        public int PropertyReferenceId { get; set; }
        public int LanguageId { get; set; }
        public string PageTitle { get; set; }
        public string Url { get; set; }
        public string MetaDescription { get; set; }
        public string Name { get; set; }
        public string Summary { get; set; }
        public string Description { get; set; }
        public string DistanceFromAirport { get; set; }
        public string TransferTime { get; set; }
        public string RightChoice { get; set; }
        public string LocationAndResort { get; set; }
        public string EatingAndDrinking { get; set; }
        public string Accomodation { get; set; }
        public string SuitableFor { get; set; }
        public string SwimmingPools { get; set; }
    }
}
