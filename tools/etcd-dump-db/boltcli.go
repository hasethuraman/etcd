package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
        "time"
	"os"

	bbolt "go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

func Buckets(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Println(err)
		return
	}

	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			fmt.Println(string(name))
			return nil
		})
	})
	if err != nil {
		fmt.Println(err)
		return
	}
}

var flockTimeout time.Duration

func dumpkeys(path string, bucket string, prefix string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Println(err)
		return
	}

	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	db.View(func(tx *bbolt.Tx) error {
		// Assume bucket exists and has keys
		decode := true
		b := tx.Bucket([]byte(bucket))

		// c := b.Cursor()

		// if prefix != "" {
		// 	prefixbytes := []byte(prefix)
		// 	for k, v := c.Seek(prefixbytes); k != nil && bytes.HasPrefix(k, prefixbytes); k, v = c.Next() {
		// 		fmt.Printf("key=%s\n", k, v)
		// 	}
		// 	return nil
		// }

		//not working
		//for k, _ := c.First(); k != nil; k, _ = c.Next() {
		//    kstring := bytes.NewBuffer(k).String()
		//    fmt.Printf("%s\n", kstring)
		//}

		//working
		b.ForEach(func(k, v []byte) error {
			//fmt.Printf("%s\n", bytes.NewBuffer(k).String())
			// fmt.Printf("key=%s, \nvalue=%s\n", bytes.NewBuffer(k), bytes.NewBuffer(v))

			if dec, ok := decoders_get[bucket]; decode && ok {
				revision, k, v, rev := dec(k, v)
				fmt.Printf("(%+v) Retrieved key=%q, value=%q, revision=%d\n", revision, k, v, rev)
			} else {
				fmt.Printf("key=%q, value=%q\n", k, v)
			}

			return nil
		})
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}
}

// IMPORTANT - dependency from etcd server code
const revBytesLen = 8 + 1 + 8
const markedRevBytesLen = revBytesLen + 1

func revToBytes(rev revision, bytes []byte) {
	binary.BigEndian.PutUint64(bytes, uint64(rev.main))
	bytes[8] = '_'
	binary.BigEndian.PutUint64(bytes[9:], uint64(rev.sub))
}

func addAKeyValue(path string, bucket string, key string, value string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Println(err)
		return
	}

	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		// Retrieve the users bucket.
		// This should be created when the DB is first opened.
		b := tx.Bucket([]byte(bucket))

		ibytes := make([]byte, revBytesLen, markedRevBytesLen)
		idxRev := revision{main: 2, sub: int64(0)}
		revToBytes(idxRev, ibytes)

		kv := mvccpb.KeyValue{
			Key:            bytes.NewBufferString(key).Bytes(),
			Value:          bytes.NewBufferString(value).Bytes(),
			CreateRevision: 0,
			ModRevision:    0,
			Version:        1,
			Lease:          int64(0),
		}

		d, err := kv.Marshal()

		if err != nil {
			fmt.Println(err)
			return err
		}

		// Persist bytes to users bucket.
		return b.Put(ibytes, d)
	})
}

// note: take a copy of db (or snapshot also) and then do otherwise it wont work
// ./boltcli listbuckets /tmp/db
// ./boltcli dumpkeys /tmp/db <bucketname> [prefix]
// ./boltcli addkey /tmp/db <bucketname> <key> <value>
func main() {
	if os.Args[1] == "listbuckets" {
		Buckets(os.Args[2])
	} else if os.Args[1] == "dumpkeys" {
		prefix := ""
		if len(os.Args) > 4 {
			prefix = os.Args[4]
		}
		dumpkeys(os.Args[2], os.Args[3], prefix)
	} else if os.Args[1] == "addkey" {
		if len(os.Args) < 6 {
			print("Usage <command> addkey /tmp/db <bucketname> <key> <value>")
			return
		}
		key := os.Args[4]
		value := os.Args[5]
		addAKeyValue(os.Args[2], os.Args[3], key, value)
	}
}
