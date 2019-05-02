Hera Overview
======================================

The High Efficiency Reliable Access (Hera) sits in between the application and the
database.  The hera-jdbc JDBC driver connects to Hera.

<img src="overview.png">

# Multiplexing

The Hera holds client connections and only uses a database connection when 
there's an active query.  Database transactions or queries with multiple
fetches aren't able to share the connection with other clients.

<img src="multiplexing.png">  

