package bot

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"project_h2b9_s8z8_y4n8/shared"
	"regexp"
	"strconv"
	"strings"
)

var SUCCESS_ZERO int = 0
var brokenTxs map[string]string
var brokenRecvTxs map[string]string

//var listOfFiles []string
var listOfFiles map[string]string
var totalFilesSize int64

////////////////////////////////////////////////////////////////////////////////
// Indexer sends file size to cnc when requested.
// return (-1, err) if there is an error and there are no files
// return (<filesize>, nil) if all is good and there are files
////////////////////////////////////////////////////////////////////////////////
func indexer() int64 {
	var totalFiles int64

	//fmt.Printf("[indexer] starting file indexer\n")
	storedfiles := "storedfiles"

	if stat, err := os.Stat(storedfiles); os.IsNotExist(err) && !stat.IsDir() {
		log.Printf("[indexer] file is not a directory %s\n ", storedfiles)
		return -1
	}

	// walk the directory
	listOfFiles = make(map[string]string)
	err := filepath.Walk(storedfiles, indexFiles)
	if err != nil {
		log.Printf("[indexer] filed to check storedfiles\n")
		return -1
	}

	// init list of files
	//listOfFiles = make([]string, len(files))
	//fmt.Println("[indexer] list of files")
	/*
		for _, v := range listOfFiles {
			fmt.Printf("%s\n", v)
		}
	*/

	//fmt.Printf("[indexer] total files size %d\n", totalFilesSize)
	return totalFiles

}

////////////////////////////////////////////////////////////////////////////////
// function used to apply to each file/dir in the tree
////////////////////////////////////////////////////////////////////////////////
func indexFiles(path string, f os.FileInfo, err error) error {
	// exists and not a directory
	if stat, _ := os.Stat(path); !stat.IsDir() {
		//fmt.Printf("found file %s; size %d\n", f.Name(), f.Size())
		listOfFiles[f.Name()] = path

		totalFilesSize += f.Size()
		return nil
	}

	return nil
}
func checkUnverifiedSentDeletions() {
	unverified := make([]shared.Transaction, 0)
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentDeletions"))
		it := b.Cursor()
		for k, v := it.First(); k != nil; k, v = it.Next() {
			txn, _ := shared.DecodeTxn(v)
			unverified = append(unverified, txn)
		}

		return nil
	})
	dbMutex.RUnlock()

	for _, txn := range unverified {
		verifyDeletedSentTxnWithCNC(txn)
	}

}

func checkUnverifiedReceivedDeletions() {
	unverified := make([]shared.Transaction, 0)
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedDeletions"))
		it := b.Cursor()
		for k, v := it.First(); k != nil; k, v = it.Next() {
			txn, _ := shared.DecodeTxn(v)
			unverified = append(unverified, txn)
		}

		return nil
	})
	dbMutex.RUnlock()

	for _, txn := range unverified {
		verifyDeletedReceivedTxnWithCNC(txn)
	}

}

func checkUnverifiedSent() {
	unverified := make([]shared.Transaction, 0)
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentTxns"))
		it := b.Cursor()
		for k, v := it.First(); k != nil; k, v = it.Next() {
			txn, _ := shared.DecodeTxn(v)
			unverified = append(unverified, txn)
		}

		return nil
	})
	dbMutex.RUnlock()

	for _, txn := range unverified {
		verifySentTxnWithCNC(txn)
	}

}

func checkUnverifiedReceived() {
	unverified := make([]shared.Transaction, 0)
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("PeerStoredFiles"))
		it := b.Cursor()
		// first we remove all files from indexer that we know have completed their transactions
		for k, v := it.First(); k != nil; k, v = it.Next() {
			frs, _ := decodeFrs(v)
			for _, fr := range frs {
				delete(listOfFiles, fr.FileName)
			}

		}
		uvst := tx.Bucket([]byte("UnverifiedReceivedTxns"))
		it = uvst.Cursor()

		listOfFilesToDelete := make([]string, 0)
		for k, path := range listOfFiles {
			var isUnverified bool = false
			for tid, v := it.First(); tid != nil; tid, v = it.Next() {
				txn, _ := shared.DecodeTxn(v)
				unverified = append(unverified, txn)
				if k == txn.FileName {
					isUnverified = true
				}
			}
			if isUnverified == false {
				listOfFilesToDelete = append(listOfFilesToDelete, path)
			}
		}
		for _, path := range listOfFilesToDelete {
			err := os.Remove(path)
			shared.CheckErrorNonFatal("Error removing file", err)
			fmt.Println("Removing file: ", path)
		}

		return nil
	})
	dbMutex.RUnlock()

	for _, txn := range unverified {
		verifyReceivedTxnWithCNC(txn)
	}

}

////////////////////////////////////////////////////////////////////////////////
// checks govector logs for previous incompleted sends
// TODO: no consideration for Y and Z time right now
////////////////////////////////////////////////////////////////////////////////
func checkSendLogs() bool {
	brokenTxs = make(map[string]string)
	// read client log file
	fmt.Println("[checkSendlog] checkLogs: Checking logs")
	fileLoc := "clientlog-rep-on-2-sameple.txt"
	if stat, err := os.Stat(fileLoc); os.IsNotExist(err) || stat.IsDir() {
		log.Println(err)
		return true
	}
	file, err := os.Open(fileLoc)
	if err != nil {
		log.Println("[checkSendlog] No logs found.")
		return true
	}

	scanner := bufio.NewScanner(file)

	var txid string
	var ipport string
	var txFilename string

	for scanner.Scan() {
		line := scanner.Text()
		match, _ := regexp.MatchString("^(Replicating on).*", line)
		if match {
			// check for closing entry
			// fmt.Println(line)
			// split line by spaces and get last two elements
			s := strings.Split(line, " ")
			txid = s[len(s)-1]
			ipport = strings.Trim(s[2], "\"")
			txFilename = strings.Trim(s[3], ":")

			mapKey := txid + "|" + ipport
			fmt.Printf("[checkSendlog] adding txid|ipport: %s, filename: %s\n",
				mapKey, txFilename)
			brokenTxs[mapKey] = txFilename
		}
	}

	/* this is actual log matching part */
	// collected log info, now need to match it with closing blocks
	// closing because scanner is not working when used for the second time
	file.Close()

	file, err = os.Open(fileLoc)
	if err != nil {
		log.Println("[checkSendlog] No logs found.")
		return true
	}

	scanner = bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("[checkSendlog] Number of txs to check %d\n", len(brokenTxs))

	// search for closing block statements and see if i have them in the hash map
	// remove every tx that is "complete" from the allTxs hashmaps.
	// What's left is incomplete transaction
	var line string
	for scanner.Scan() {
		line = scanner.Text()
		match, _ := regexp.MatchString("^(txid).*", line)
		if match {
			// fmt.Println(line)
			s := strings.Split(line, " ")
			txid := s[1]
			receiver := strings.Trim(s[2], "\"")
			success, _ := strconv.Atoi(s[4])

			// if matched delete from the hash map because it is a complete tx
			mapKey := txid + "|" + receiver
			fmt.Printf("[checkSendlog] Rm completed txid|ipport %s, success %d\n",
				mapKey, success)
			delete(brokenTxs, mapKey)
		}

	}
	// when this func terminates we are left with a hashmap of incomplete blocks

	// TODO
	// if block is incomplete - dial CNC and see if the file is verified
	fmt.Printf("[checkSendlog] Number of incomplete txs %d\n", len(brokenTxs))
	for k, fn := range brokenTxs {
		fmt.Printf("txid|ipport %s; filename %s\n", k, fn)
		s := strings.Split(k, "|")
		txid, _ := strconv.ParseUint(s[0], 10, 64)
		var transaction shared.Transaction
		dbMutex.RLock()
		dberr := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("CloudStoredFiles"))
			btxns := b.Get([]byte(fn))
			if btxns == nil {
				Logger.LogLocalEvent(fmt.Sprintf("txid %d \"%v\" replied: %d\n", s[0], s[1], SUCCESS_ZERO))
				return nil
			} else {
				txns, _ := decodeTxns(btxns)
				for _, txn := range txns {
					if txn.TransactionID == txid {
						transaction = txn
						return errors.New("Transaction in inconsistent state")
					}
				}
			}
			return nil
		})
		dbMutex.RUnlock()

		if dberr != nil {
			//Dial the receiver to check
			cbc, err := rpc.Dial("tcp", cncrpcip)
			var r shared.Status
			if err != nil {
				continue
			}
			err = cbc.Call("CNCBotCommunication.VerifyTransaction", transaction, &r)
			if r.Success == 0 { // delete our record of the transaction
				dbMutex.Lock()
				db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("CloudStoredFiles"))
					btxns := b.Get([]byte(fn))
					txns, _ := decodeTxns(btxns)
					for i, txn := range txns {
						if txn.TransactionID == txid {
							// delete the transaction and return
							txns = append(txns[:i], txns[(i+1):]...)
							btxns, _ = encodeTxns(txns)
							b.Put([]byte(fn), btxns)
							return nil
						}
					}
					Logger.LogLocalEvent(fmt.Sprintf("txid %d \"%v\" replied: %d\n", s[0], s[1], 0))
					return nil
				})
				dbMutex.Unlock()
			} else {
				Logger.LogLocalEvent(fmt.Sprintf("txid %d \"%v\" replied: %d\n", s[0], s[1], 1))

			}

		}

		// TODO remove the entry from the log
		// TODO print on screen that those txs failed
		// write to log file that the tx failed - closes block so it wont be picked
		// up by the parser on the next startup
		//Logger.LogLocalEvent(fmt.Sprintf("txid %d \"%v\" replied: %d\n", txid,, botAddr, SUCCESS_ZERO))
	}

	file.Close()
	if len(brokenTxs) != 0 {
		return false
	} else {
		return true
	}

}

////////////////////////////////////////////////////////////////////////////////
// checks govector logs for previous incompleted sends
// TODO: no consideration for Y and Z time right now
////////////////////////////////////////////////////////////////////////////////
func checkRecvLogs() bool {
	brokenRecvTxs = make(map[string]string)
	// read client log file
	fmt.Println("[checkRecvlog] checkLogs: Checking logs")
	fileLoc := "bot-receive.log"
	if stat, err := os.Stat(fileLoc); os.IsNotExist(err) || stat.IsDir() {
		log.Println(err)
		return true
	}
	file, err := os.Open(fileLoc)
	if err != nil {
		log.Println("[checkRecvlog] No logs found.")
		return true
	}

	scanner := bufio.NewScanner(file)

	var txid string
	var ipport string
	var filename string

	// READ LOGS INTO THE HASH MAP
	for scanner.Scan() {
		line := scanner.Text()
		match, _ := regexp.MatchString("^(receiving=).*", line)
		if match {
			// check for closing entry
			// fmt.Println(line)
			// split line by spaces and get last two elements
			s := strings.Split(line, " ")
			filename = s[1]
			ipport = s[2]
			txid = s[len(s)-1]

			mapKey := txid + "|" + ipport
			fmt.Printf("[checkRecvlog] adding txid|ipport: %s, filename: %s\n",
				mapKey, filename)
			// brokenRecvTxs[mapKey] = filename
			brokenRecvTxs[filename] = mapKey
		}
	}
	file.Close()

	file, err = os.Open(fileLoc)
	if err != nil {
		log.Println("[checkRecvlog] No logs found.")
		return true
	}

	scanner = bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("[checkRecvlog] Number of txs to check %d\n", len(brokenRecvTxs))

	// VERIFY LOGS
	// search for closing block statements and see if i have them in the hash map
	// remove every tx that is "complete" from the broken hashmaps.
	// What's left is incomplete transaction
	var sender string
	var line string
	for scanner.Scan() {
		line = scanner.Text()
		match, _ := regexp.MatchString("^(OK ).*", line)
		if match {
			s := strings.Split(line, " ")
			txid = s[2]
			filename = s[3]
			sender = s[4]

			// if matched delete from the hash map because it is a complete tx
			mapKey := txid + "|" + sender
			fmt.Printf("[checkRecvlog] Rm completed txid|ipport %s, filename %s\n",
				mapKey, filename)
			// delete(brokenRecvTxs, mapKey)
			delete(brokenRecvTxs, filename)
		}

	}

	// ask each receiver to check if they have the file
	// if block is incomplete - dial receivers and see if they have the file
	file.Close() // opening 3 times is bad, but I don't have time to make this better
	file, err = os.OpenFile(fileLoc, os.O_APPEND|os.O_WRONLY, 0600)

	var logEntryBuffer bytes.Buffer
	var logEntry string
	fmt.Printf("[checkRecvlog] Number of incomplete txs %d\n", len(brokenRecvTxs))

	for fInd, _ := range listOfFiles {
		fmt.Printf("[checkRecvlog] checking %s\n", fInd)

		// file in the indexer and in the log - call sender
		// This works i think, the else case does not
		if idSender, foundInLog := brokenRecvTxs[fInd]; foundInLog {
			sender = strings.Split(idSender, "|")[1]
			fmt.Printf("[checkRecvlog] file in indexer and in the log %s; sender %s\n", fInd, sender)

			// TODO dial sender
		} else {
			// file is in the indexer and not in the log - remove the file
			// because we never send the verification and sender will assume that file is deleted.
			// add closing block and remove the file
			fmt.Printf("[checkRecvlogs] file %s is in indexer and not log: removing. Closing log entry.\n", fInd)
			logEntryBuffer.WriteString("OK txid= " + txid + " ")
			logEntryBuffer.WriteString(fInd + " " + sender)
			logEntryBuffer.WriteString(" TX_ABORTED_AND_FILE_REMOVED" + "\n")
			logEntry = logEntryBuffer.String()

			if _, err = file.WriteString(logEntry); err != nil {
				fmt.Println("[checkRecvlogs] failed to write log entry: "+logEntry, err)
			}
			logEntryBuffer.Reset()
			nodeid := getNodeIdFromFilename(fInd)
			// remove the file
			err = os.Remove("storedfiles/" + nodeid + "/" + fInd)
			if err != nil {
				log.Println("[checkRecvLogs] ", err)
			} else {
				log.Printf("[checkRecvLogs] file %s removed.\n", fInd)
			}
		}

	}

	file.Close()
	if len(brokenRecvTxs) != 0 {
		return false
	} else {
		return true
	}

}

////////////////////////////////////////////////////////////////////////////////
// return node id from the filename
////////////////////////////////////////////////////////////////////////////////
func getNodeIdFromFilename(filename string) string {
	s := strings.Split(filename, "|")
	return s[1]
}

/*
	TODO
	- compare against files i have on disk

	if file is in the indexer, but not in the log
		remove the file
	if file is in the indexer and in the log
		verify the file and tx

		YES - keep the file; update db; update the log; DONE
		NO - remove the file; update the log; DONE

	use RPC to dial CNC
*/

/*
fail case
sender failing but the reciver does not know that it happened. This means that we cannot come back to the sender and tell it that the tx went throught. Sender will reboot and check its logs and it will have to dial every receiver of a broken tx. We could avoid it if we just agreed that we would delete the file if either party died during tx (annul the tx everywhere)
*/

/*
NOTES
logs to try
File in the log AND in the indexer

receiving= gHFbQb2bptFKEjEbSwyyabjy+zmv5OvmoQ7JwlLrvr8=|111|file2 127.0.0.1:10001 nodeid= 111 txid= 7



File in the indexer AND not in the log
just put one file in some folder in storedfiles/ and nothing in the log
*/
