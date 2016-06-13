# ExternalDBMapper
A truly "down and dirty" utility that, based on a SQL database, will generate (mostly) the scripts required to import it and all tables as external datasources and tables, respectively.

    Usage: ExternalDBMapper <-s,-u,-p,-d,-e,[-es,-c,-o,-v, -t:<table1, table2...>]>
      Args are in the format of -<arg>:<value> unless otherwise noted
      
      -s    Server name
      -u    Username to connect to the specified server
      -p    Password  
      -d    Database for which scripts are generated
      -e    The elastic datasource the tables should be created with
      -c    Name of the credential used for the Azure SQL database
      -es   Optionally specifies that we should generate the external datasource
      -v    Verbose - not much here, just dumps the DDL to the console window if -o is specified
      -o    Output file path - relative paths supported
      -t    An optional list of tables delimitted by ','. If specified only the scripts for these tables are generated. Exact match.

This was the byproduct of getting stuck in traffic this AM - enjoy!
