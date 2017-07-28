package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/ralreegorganon/nmeaais"
)

func main() {
	files, err := filepath.Glob("*.zip")
	if err != nil {
		log.Fatal(err)
	}

	d := nmeaais.NewDecoder()

	db, err := sqlx.Open("postgres", "postgres://ino:ino@localhost:9432/ino?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	donezo := make(chan bool)
	go func() {
		txn, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("message", "mmsi", "type", "message", "raw", "created_at"))
		if err != nil {
			log.Fatal(err)
		}

		for o := range d.Output {
			if o.Error != nil {
				fmt.Println("couldnt decode message: ", o.Error)
				for _, p := range o.SourcePackets {
					fmt.Println(p.Raw)
				}
				continue
			}

			message, err := json.Marshal(o.DecodedMessage)
			if err != nil {
				fmt.Println("couldn't marshal message: ", err)
				message = []byte("{}")
			}

			var rawBuf bytes.Buffer
			length := len(o.SourcePackets)
			for i, p := range o.SourcePackets {
				rawBuf.WriteString(p.Raw)
				if i+1 != length {
					rawBuf.WriteString("\n")
				}
			}

			raw := rawBuf.Bytes()

			_, err = stmt.Exec(o.SourceMessage.MMSI, o.SourceMessage.MessageType, string(message), string(raw), o.Timestamp)
			if err != nil {
				log.Fatal(err)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}

		err = stmt.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}

		donezo <- true
	}()

	fl := len(files)
	for fc, z := range files {
		fmt.Printf("processing file %d of %d: %s\n", fc+1, fl, z)
		zr, err := zip.OpenReader(z)
		if err != nil {
			log.Fatal(err)
		}
		defer zr.Close()

		for _, f := range zr.File {
			rc, err := f.Open()
			if err != nil {
				log.Fatal(err)
			}
			defer rc.Close()

			reader := csv.NewReader(rc)
			reader.FieldsPerRecord = -1
			records, err := reader.ReadAll()
			if err != nil {
				log.Fatal(err)
			}

			layout := "2006-01-02 15:04:05"

			txn, err := db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			stmt, err := txn.Prepare(pq.CopyIn("packet", "raw", "created_at"))
			if err != nil {
				log.Fatal(err)
			}

			rl := len(records)
			fmt.Printf("%d rows in file\n", rl)
			for i, rec := range records {
				if i%10000 == 0 {
					fmt.Printf("%d rows processed of %d\n", i, rl)
				}

				if i == 0 || i == 1 {
					continue
				}

				if len(rec) != 13 {
					fmt.Println("fuck not the right number of columns: ", len(rec))
					break
				}

				t, err := time.Parse(layout, rec[2])
				if err != nil {
					fmt.Println("fuck bad dates: ", rec[2])
					break
				}

				s := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s", rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10])

				_, err = stmt.Exec(s, t)
				if err != nil {
					log.Fatal(err)
				}

				d.Input <- nmeaais.DecoderInput{Input: s, Timestamp: t}
			}

			_, err = stmt.Exec()
			if err != nil {
				log.Fatal(err)
			}

			err = stmt.Close()
			if err != nil {
				log.Fatal(err)
			}

			err = txn.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	close(d.Input)
	<-donezo
}
