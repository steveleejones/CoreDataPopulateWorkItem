using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreDataPopulateWorkItem.Models
{
    internal class Language
    {
        public int LanguageId { get; set; }
        public string LanguageCode { get; set; }
        public string LanguageName { get; set; }
        public string CultureCode { get; set; }
        public string Encoding { get; set; }
    }
}
