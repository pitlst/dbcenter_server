#include <iostream>
#include <istream>
#include <ostream>
#include <string>
#include <exception>

#include "soci/soci.h"
#include "soci/odbc/soci-odbc.h"

#include <Windows.h>

int main()
{
    // try
    // {
    //     // "Driver={PostgreSQL Unicode(x64)};Server=localhost;Port=5432;Database=postgres;UID=postgres;PWD=Swq8855830."
    //     // "Driver={MySQL ODBC 9.1 Unicode Driver};Server=localhost;Port=3306;Database=mysql;UID=root;PWD=Swq8855830."
    //     soci::session sql(soci::odbc, "Driver={MySQL ODBC 9.1 Unicode Driver};Server=localhost;Port=3306;Database=mysql;UID=root;PWD=123456");
    //     int count = 0;
    //     sql << "select 1", soci::into(count);;
    //     std::cout << count << "\n";
    // }
    // catch (std::exception const &e)
    // {
    //     std::cerr << "Error: " << e.what() << '\n';
    // }

    try
    {
        soci::row r;
        soci::session sql(soci::odbc, "Driver={MySQL ODBC 9.1 Unicode Driver};Server=localhost;Port=3306;Database=dataframe_flow_v2;UID=root;PWD=123456;CHARSET=utf8mb4");
        sql << "select * from dm_abnormal", soci::into(r);

        std::stringstream doc;
        doc << "<row>" << std::endl;
        for (std::size_t i = 0; i != r.size(); ++i)
        {
            const soci::column_properties &props = r.get_properties(i);

            doc << '<' << props.get_name() << '>';
            switch (props.get_data_type())
            {
                case soci::dt_double:
                    doc << r.get<double>(i, 0.);
                    break;
                case soci::dt_integer:
                    doc << r.get<int>(i, 0);
                    break;
                case soci::dt_long_long:
                    doc << r.get<long long>(i, 0);
                    break;
                case soci::dt_unsigned_long_long:
                    doc << r.get<unsigned long long>(i, 0);
                    break;
                case soci::dt_string:
                    doc << r.get<std::string>(i, "");
                    break;
                case soci::dt_date:
                    std::tm when = r.get<std::tm>(i);
                    doc << asctime(&when);
                    break;
            }
            doc << "</" << props.get_name() << '>' << std::endl;
        }
        doc << "</row>";
        std::cout << doc.str() << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << '\n';
    }
}