using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace MarkingCompare
{
    class Program
    {
        static void Main(string[] args)
        {
            Connection con = new Connection();
            con.ExecuteScalar("TRUNCATE TABLE dly_detail");
            con.sqlbulkcopy("dly_detail","dly_detail");
        }
    }
}
