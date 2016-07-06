package main

import (
	"flag"
	"fmt"
	"os"
	"project_h2b9_s8z8_y4n8/bot"
	"project_h2b9_s8z8_y4n8/cnc"
)

func main() {
	/* READ COMMAND LINE ARGUMENTS */
	botcommand := flag.NewFlagSet("bot", flag.ExitOnError)
	botid := botcommand.String("id", "", "unique node id")
	localAddress := botcommand.String("local", "", "local ip and port")
	cncrpc := botcommand.String("cncrpc", "localhost:30001", "ip port of cnc rpc address provided to bot (default = localhost:30001)")

	cnccommand := flag.NewFlagSet("cnc", flag.ExitOnError)
	cncrpcip := cnccommand.String("rpcip", "localhost:30001", "ip:port for rpc")
	replications := cnccommand.Uint64("r", 2, "amount of replications for the service (default = 2)")
	maxfilesize := cnccommand.Uint64("fs", 1000, "max file size (in bytes) supported by the service (default = 1000)")
	if len(os.Args) == 1 {
		fmt.Println("usage: go run project.go <mode> <args>")
		fmt.Println("<mode> is one of: ")
		fmt.Println("bot - Run the bot")
		fmt.Println("cnc - Run the cnc")
		return
	}

	switch os.Args[1] {
	case "bot":
		if len(os.Args) < 4 {
			fmt.Println("usage: go run project.go bot -id=<nodeid> -local=<localIP> -cncrpc<cncrpcip>")
		} else {
			botcommand.Parse(os.Args[2:])
			if *botid == "" {
				fmt.Println("Please supply a unique id for the bot")
				return
			}
			if *localAddress == "" {
				fmt.Println("Please supply a local address to listen on")
				return
			}
			bot.RunBot(*botid, *cncrpc, *localAddress)
		}
	case "cnc":
		if len(os.Args) < 2 {
			fmt.Println("usage: go run project.go cnc -local=<localIP> -r=<replicationfactor> -fs=<maxfilesize>")
		} else {
			cnccommand.Parse(os.Args[2:])
			cnc.RunCnc(*cncrpcip, *replications, *maxfilesize)
		}
	default:
		fmt.Println("usage: go run project.go <mode> <args>")
		fmt.Println("<mode> is one of: ")
		fmt.Println("bot - Run the bot")
		fmt.Println("cnc - Run the cnc")
		return

	}

}
