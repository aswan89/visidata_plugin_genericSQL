# Visidata Plugin - genericSQL

Plugin for [Visidata](https://github.com/saulpw/visidata) enabling SQL databases to be used as data sources.

Currently the plugin has been tested against Oracle and MySQL. 
The plugin can handle MS SQL Databases but the functionality has not been tested. 

To connect to a target database, simply use a [SQL ALchemy friendly connection URL](https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls) as the first argument to `vd`. 
