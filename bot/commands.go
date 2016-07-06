package bot

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"project_h2b9_s8z8_y4n8/shared"
	"strconv"
	"strings"
)

func ReadFromStdIn() {
	fmt.Println("Commands Available: ")
	fmt.Println("GET, SEND, DELETE, HELP, LIST, PFILES")
	reader := bufio.NewReader(os.Stdin)
	for {
		bytes, _, _ := reader.ReadLine()
		line := string(bytes)
		args := strings.Split(line, " ")
		handleCommand(args[:])
	}

}

func handleCommand(s []string) {
	command := strings.ToUpper(s[0])
	switch command {
	case "GET":
		if len(s) == 3 {
			getCommand(s[1], s[2])
		} else {
			fmt.Println("GET Usage: GET filename ipaddr")
		}
	case "SEND":
		if len(s) == 2 {
			sendCommand(s[1], 3)
		} else if len(s) == 3 {
			rfactor, err := strconv.ParseUint(s[2], 10, 64)
			shared.CheckErrorNonFatal("Parsing error: ", err)
			sendCommand(s[1], rfactor)
		} else {
			fmt.Println("SEND Usage: SEND file [replications]")
		}
	case "HELP":
		helpCommand()
	case "FIND":
		fmt.Println("Find command")
	case "LIST":
		listCommand()
	case "PFILES":
		listPeerFilesCommand()
	case "DELETE":
		if len(s) == 3 {
			deleteCommand(s[1], s[2])
		} else {
			fmt.Println("Delete Usage: DELETE filename IP")
		}
	default:
		fmt.Println("Unrecognized command, type HELP for a list of commands")

	}

}

func getCommand(filename, ipaddr string) {
	client, err := rpc.Dial("tcp", ipaddr)
	if err != nil {
		log.Println("Client could not be reached: ", err)
		return
	}
	var rep FileTransaction
	fr := FileRequest{botInfo.NodeID, botInfo.IPport, filename, 0}
	err = client.Call("Bot2BotCommunication.RetrieveFile", fr, &rep)
	if err != nil {
		log.Println("Could not retrieve file: ", err)
		client.Close()
		return
	}
	if rep.FileName == "null" || rep.FileName == "" {
		log.Println("Your file was not found on the remote: ", ipaddr)
		return
	}
	saveloc := "retrievedfiles"
	fmt.Println("Will save in default location: retreivedfiles")

	// decrypted file as []byte
	plainFile := decrypt(rep.File)

	// Check if location exists
	if stat, err := os.Stat(saveloc); os.IsNotExist(err) || !stat.IsDir() {
		log.Println(err)
		client.Close()
		return
	} else {
		// Location exists, but need to check if file with name already exists
		fn := filepath.Join(saveloc, rep.FileName)
		if _, err := os.Stat(fn); os.IsExist(err) {
			log.Println(err)
			client.Close()
			return
		} else {
			err = ioutil.WriteFile(fn, plainFile, 0666) // use this to write decrypted file.
			fmt.Println("Filename: ", rep.FileName)
			if err != nil {
				log.Println(err)
				client.Close()
				return
			}
			// i think this should work, but i could not test it
			//f.Write(rep.File) // write decrypted file instead of this
			log.Println("File saved at: ", saveloc)
		}
	}

	client.Close()
	return

}

////////////////////////////////////////////////////////////////////////////////
// SEND the file specified at the command line to another bot
////////////////////////////////////////////////////////////////////////////////
func sendCommand(fileLoc string, numreplications uint64) {
	// Logger.PrepareSend // create a log here as well and use it for redo logging
	if stat, err := os.Stat(fileLoc); os.IsNotExist(err) || stat.IsDir() {
		log.Println(err)
		return
	}
	fbytes, err := ioutil.ReadFile(fileLoc)
	if err != nil {
		log.Println(err)
		return
	}
	_, fn := filepath.Split(fileLoc)
	replicateFile(fbytes[:], numreplications, fn)
}

//Lists all the files that are located in the "cloud"
func listCommand() {
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("CloudStoredFiles"))
		it := b.Cursor()
		var buf bytes.Buffer
		for k, v := it.First(); k != nil; k, v = it.Next() {
			buf.Reset()
			fmt.Printf("%s: ", k)
			txns, _ := decodeTxns(v)
			for _, txn := range txns {
				buf.WriteString(txn.ReceiverIP + ", ")
			}
			l := strings.TrimSuffix(buf.String(), ", ")
			fmt.Println(l)

		}

		return nil
	})
	dbMutex.RUnlock()

}

func listPeerFilesCommand() {
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("PeerStoredFiles"))
		it := b.Cursor()
		var buf bytes.Buffer
		for k, v := it.First(); k != nil; k, v = it.Next() {
			buf.Reset()
			fmt.Printf("%s: ", k)
			frs, _ := decodeFrs(v)
			for _, fr := range frs {
				buf.WriteString(fr.FileName + ", ")
			}
			l := strings.TrimSuffix(buf.String(), ", ")
			fmt.Println(l)

		}

		return nil
	})
	dbMutex.RUnlock()

}

func deleteCommand(filename, ip string) {
	var deltxn shared.Transaction // transaction to delete
	dbMutex.RLock()
	dberr := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("CloudStoredFiles"))
		it := b.Cursor()
		for k, v := it.First(); k != nil; k, v = it.Next() {
			fn := string(k)
			if fn == filename {
				txns, _ := decodeTxns(v)
				for _, txn := range txns {
					if txn.ReceiverIP == ip {
						deltxn = txn
						return nil
					}
				}
			}
		}

		return errors.New("No such transaction exists")
	})
	dbMutex.RUnlock()
	if dberr != nil {
		shared.CheckErrorNonFatal("Deletion: ", dberr)
		return
	}
	fileDeletion(deltxn)

}

func helpCommand() {
	fmt.Println("GET Usage: GET filename ipaddr - Gets a stored file on another computer")
	fmt.Println("SEND Usage: SEND file [replications] - Sends a file to another computer with optional replications. Replications are capped by the CNC")
	fmt.Println("DELETE Usage: DELETE filename ipaddr - Deletes a stored file on another computer")
	fmt.Println("LIST - Lists the filename and IP location of files stored on other computers")
	fmt.Println("PFILES - Lists the filename and IP origin of files received from other computers")

}
