RESQL is a REST interface for SQL databases and other data formats.
The name comes from a contraction of REST and SQL acronyms.
The application listens to a configured HTTP port and delivers available data.
Its primary goal is to expose SQL databases with a REST API.
CRUD interface is available with POST, GET, PUT and DELETE HTTP verbs.
The application can also deliver local folders and specific data loaded in memory from local or remote storage.
To start the server, just execute run-server.sh with the configuration data as argument
You can also use REPL interface with the resql.sh script
Be careful not to deploy this application in production without appropriate security checks.

