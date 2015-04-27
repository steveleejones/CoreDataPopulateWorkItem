using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreDataPopulateWorkItem.Models
{
    internal class PropertyUrl
    {
        public int PropertyReferenceId { get; set; }
        public int BrandID { get; set; }
        public int LanguageID { get; set; }
        public string URL { get; set; }
        public string RedirectURL { get; set; }
    }
}
