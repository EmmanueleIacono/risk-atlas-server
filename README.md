# RiskAtlas
This is the back-end side of the RiskAtlas web platform.  

### A few notes on configuration
#### Database and Server config
In order to connect to a database, this project requires the presence of a `config.ini` file, placed in the root of the project, and containing the following:  
```
[database]
url = postgresql://database_user:database_pw@database_host:database_port/database_name

[server]
host = your.own.server.ip (e.g. 127.0.0.1)
port = your port number of choice (e.g. 3000)

```

#### Serving the application
Simply compiling the application and placing the executable (together with the `config.ini` file) on a server should allow it to be served correctly.  
