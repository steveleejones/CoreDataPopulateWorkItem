using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreDataPopulateWorkItem.Models
{
    internal class PropertyDetails
    {
        public int PropertyReferenceId { get; set; }
        public int LanguageId { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string TownCity { get; set; }
        public string County { get; set; }
        public string Telephone { get; set; }
        public string Fax { get; set; }
        public string PostcodeZip { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public int PropertyTypeId { get; set; }
        public string PropertyType { get; set; }
        public string AirportCode { get; set; }
        public string Strapline { get; set; }
        public string MainImage { get; set; }
        public string MainImageThumbnail { get; set; }
    }
}
