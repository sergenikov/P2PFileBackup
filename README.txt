This project requires both the CNC and a bot to be running (order of startup does NOT matter)

The project.go file located in the project/ folder is responsible for running either the cnc or the bot based on the flags passed:

If the cnc is run then

go run project.go cnc 

will run the cnc on localhost:30001 (default) or you can choose a specific ip and port using the "rpcip" flag like so:

go run project.go cnc -rpcip="IP:PORT"

Additional optional flags:
-r is the number of maximum replications that a client can request (default is 2)
-fs is the maximum file size that the cnc will accept before issuing a transaction (default is 1000)

NOTE: the default storage allocated for all nodes on startup is 15000 bytes. That is how much each node is allowed to store in the network and how much available space
a node must provide to join the network. This default can be changed in cnc/cnc.go by changing the "INITIALSTORAGE" variable

The bot can be run with the following flags: 

go run project.go bot -id=2 -local=localhost:4000  -cncrpc=localhost:30001

-id is a unique id for this node
-local is the localip (for incoming rpc calls)
-cncrpc is the rpc ip:port of the cnc

You can run many bots and a single cnc from the same project folder. Just be aware that files are stored in a "storedfiles" directory created upon startup, and each client is expecting to have their own copy of the "storedfiles" directory so it might be better to clone the repo as many times as clients you wish to run. Note that the CNC can be run in the same directory as a client without any issue
One a bot is running, you can type "help" to get a description of commands and their usage instructions
