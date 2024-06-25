// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark put",

	Run: putFunc,
}

type kv struct {
	key   string
	value string
}

var (
	keySize int
	valSize int

	putTotal int
	putRate  int

	keySpaceSize int
	seqKeys      bool
	mayastor     bool

	compactInterval   time.Duration
	compactIndexDelta int64

	checkHashkv bool

	saveKeysToFile string

	msnodes      int
	msdisks      int
	mssnaps      int
	noofsegs     int
	noofrepls    int
	noofrebuilds int
	noofsnaps    int

	q = make(chan ([]kv), 100000)
)

func init() {
	RootCmd.AddCommand(putCmd)
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")

	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	putCmd.Flags().StringVar(&saveKeysToFile, "saveKeysToFile", "", "Save keys to file")
	putCmd.Flags().BoolVar(&mayastor, "mayastor", false, "Is mayastor testing")
	putCmd.Flags().IntVar(&msnodes, "msnodes", 10, "Total number of nodes when mayastor flag is set")
	putCmd.Flags().IntVar(&msdisks, "msdisks", 10, "Total number of disks across msnodes when mayastor flag is set")
	putCmd.Flags().IntVar(&mssnaps, "mssnaps", 0, "Total number of snapshots across msnodes when mayastor flag is set")
	putCmd.Flags().IntVar(&noofsegs, "noofsegs", 16384, "Number of segs in a volume")
	putCmd.Flags().IntVar(&noofsnaps, "noofsnaps", 0, "Number of segs in a volume")
	putCmd.Flags().IntVar(&noofrebuilds, "noofrebuilds", 0, "Number of segs in a volume")
	putCmd.Flags().IntVar(&noofrepls, "noofrepls", 1, "Number of replications in a volume")
	putCmd.Flags().BoolVar(&seqKeys, "sequential-keys", false, "Use sequential keys")
	putCmd.Flags().DurationVar(&compactInterval, "compact-interval", 0, `Interval to compact database (do not duplicate this with etcd's 'auto-compaction-retention' flag) (e.g. --compact-interval=5m compacts every 5-minute)`)
	putCmd.Flags().Int64Var(&compactIndexDelta, "compact-index-delta", 1000, "Delta between current revision and compact revision (e.g. current revision 10000, compact at 9000)")
	putCmd.Flags().BoolVar(&checkHashkv, "check-hashkv", false, "'true' to check hashkv")
}

func generateRandomIP() string {
	rand.Seed(time.Now().UnixNano())
	octet1 := rand.Intn(256)
	octet2 := rand.Intn(256)
	octet3 := rand.Intn(256)
	octet4 := rand.Intn(256)
	return fmt.Sprintf("%d.%d.%d.%d", octet1, octet2, octet3, octet4)
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result string
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(charset))
		result += string(charset[randomIndex])
	}

	return result
}

func generateRandomStringWith0s1s(length int) string {
	const charset = "01"
	var result string
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(charset))
		result += string(charset[randomIndex])
	}

	return result
}

func populate_kvs(clusterid string) {
	cn := uint(0)

	kvs := []kv{}
	for i := 0; i < msnodes; i++ {
		k1 := fmt.Sprintf(clusterid+"/namespaces/default/NodeSpec/k8s-agentpool1-10232180-vmss00%05d", i)
		v1 := fmt.Sprintf(
			"{\"id\":\"k8s-agentpool1-10232180-vmss00%05d\",\"endpoint\":\"%s:10124\",\"labels\":{\"topology.kubernetestorage.csi.mayastor.com/zone\":\"\"},\"cordon_labels\":[]",
			i,
			generateRandomIP(),
		)
		kvs = append(kvs, kv{key: k1, value: v1})
	}
	if len(kvs) > 0 {
		q <- kvs
	}

	kvs = []kv{}
	for i := 0; i < msdisks; i++ {
		cn = cn + 1
		if cn >= uint(msnodes) {
			cn = 0
		}
		agentpoold := fmt.Sprintf("k8s-agentpool1-10232180-%d", cn)
		k1 := clusterid + "/namespaces/default/PoolSpec/csi-" + generateRandomString(5)
		v1 := fmt.Sprintf(
			"{\"node\":\"%s\",\"id\":\"csi-abcde\",\"disks\":[\"/abc/dis\"],\"status\":{\"Created\":\"Online\"},\"labels\":{\"default.cloud.org/storagedisk\":\"testdiskpoolfromcloud-request-xyz\",\"default.cloud.org/provisionedby\":\"someone-by-administring\",\"default.cloud.org/somepool\":\"testdiskpoolfromcloud-request-xyz-diskpool-abcde\",\"default.cloud.org/plumber\":\"somediskpool-provisioned-24gdd\",\"openebs.io/created-by\":\"operator-diskpool\",\"default.cloud.org/type-of-disk\":\"generallycloud\"},\"operation\":null,\"pooltype\":\"any\",\"revision\":300}",
			agentpoold,
		)
		kvs = append(kvs, kv{key: k1, value: v1})
	}
	if len(kvs) > 0 {
		q <- kvs
	}

	rebuild_map_str := generateRandomStringWith0s1s(noofsegs)
	rebuild_count := noofrebuilds
	snap_count := noofsnaps

	cn = uint(0)
	for i := 0; i < putTotal; i++ {

		agentpoold := fmt.Sprintf("k8s-agentpool1-10232180-%d", cn)
		cn = cn + 1
		if cn >= totalClients {
			cn = 0
		}
		nexid := uuid.New().String()
		volid := uuid.New().String()

		k1 := clusterid + "/namespaces/default/VolumeSpec/" + volid
		v1 := fmt.Sprintf(
			"{\"uuid\":\"%s\",\"size\":2147483648,\"labels\":null,\"num_replicas\":1,\"status\":{\"Created\":\"Online\"},\"target\":{\"node\":\"%s\",\"nexus\":\"%s\",\"protocol\":\"nvmf\"},\"policy\":{\"self_heal\":true},\"topology\":{\"node\":null,\"pool\":{\"Labelled\":{\"exclusion\":{},\"inclusion\":{\"openebs.io/created-by\":\"operator-diskpool\"}}}},\"last_nexus_id\":\"%s\",\"operation\":null,\"thin\":false}",
			volid, agentpoold, nexid, nexid,
		)
		kvs := []kv{}
		kv1 := kv{}
		kv1.key = k1
		kv1.value = v1
		kvs = append(kvs, kv1)

		replids := []string{}
		for r := 0; r < noofrepls; r++ {
			replid := uuid.New().String()
			replids = append(replids, replid)
			k2 := clusterid + "/namespaces/default/ReplicaSpec/" + replid
			v2 := fmt.Sprintf(
				"{\"name\":\"%s\",\"uuid\":\"%s\",\"size\":2147483648,\"pool\":\"diskpool-ppndp\",\"share\":\"none\",\"thin\":false,\"status\":{\"Created\":\"online\"},\"managed\":true,\"owners\":{\"volume\":\"%s\"},\"operation\":null}",
				replid, replid, volid,
			)

			kv2 := kv{}
			kv2.key = k2
			kv2.value = v2

			kvs = append(kvs, kv2)
		}

		if rebuild_count > 0 {
			rebuild_count = rebuild_count - 1
		}
		if rebuild_count == 0 {
			rebuild_map_str = ""
		}
		k3 := clusterid + "/namespaces/default/NexusSpec/" + nexid
		k4 := clusterid + "/namespaces/default/volume/" + volid + "/nexus/" + nexid + "/info"
		n_child_str := ""
		child_str := ""
		l_rebuild_str := rebuild_map_str
		for _, replid := range replids {
			n_child_str = n_child_str + fmt.Sprintf("{\"Replica\":{\"uuid\":\"%s\",\"share_uri\":\"nvmf://10.1.0.5:8420/nqn.2019-05.io.openebs:/xfs-disk-pool/loopxyz2023070702/%s?uuid=%s\"}},", replid, replid, replid)
			child_str = child_str + fmt.Sprintf("{\"healthy\":true,\"uuid\":\"%s\",\"rebuild_map\":%s},", replid, l_rebuild_str)
			l_rebuild_str = ""
		}
		v3 := fmt.Sprintf(
			"{\"uuid\":\"%s\",\"name\":\"%s\",\"node\":\"%s\",\"children\":[%s],\"size\":2147483648,\"spec_status\":{\"Created\":\"Online\"},\"share\":\"nvmf\",\"managed\":true,\"owner\":\"%s\",\"operation\":null}",
			nexid, volid, agentpoold, n_child_str, volid,
		)
		v4 := fmt.Sprintf(
			"{\"children\":[%s],\"clean_shutdown\":false}",
			child_str,
		)
		kv3 := kv{}
		kv3.key = k3
		kv3.value = v3

		kv4 := kv{}
		kv4.key = k4
		kv4.value = v4

		kvs = append(kvs, kv3)
		kvs = append(kvs, kv4)

		if snap_count == 0 {
			mssnaps = 0
		}

		if mssnaps > 0 {
			snapid := uuid.New().String()
			k5 := clusterid + "/namespaces/default/VolumeSnapshotSpec/" + volid + "@snapshot-" + snapid
			v5 := fmt.Sprintf(
				"{\"name\":\"%s\",\"volume_id\":\"%s\",\"nexus_id\":\"%s\",\"creation_timestamp\":\"2024-06-24T11:50:02.516046460Z\",\"status\":{\"Created\":null},\"operation\":null,\"revision\":200}",
				volid+"@snapshot-"+snapid,
				volid,
				nexid,
			)
			kv5 := kv{}
			kv5.key = k5
			kv5.value = v5
			kvs = append(kvs, kv5)

			for j := 0; j < mssnaps; j++ {
				repid := uuid.New().String()
				repname := uuid.New().String()
				k6 := clusterid + "/namespaces/default/ReplicaSnapshotSpec/" + repid
				v6 := fmt.Sprintf(
					"{\"name\":\"%s\",\"uuid\":\"%s\",\"snapshot_name\":\"%s\",\"source_id\":\"%s\",\"pool_id\":\"local-gll2d\",\"owner\":{\"volume_snapshot\":\"%s\"},\"status\":{\"Created\":null},\"operation\":null,\"revision\":200}",
					repname+"@snapshot-"+snapid,
					repid,
					volid+"@snapshot-"+snapid,
					repname,
					volid+"@snapshot-"+snapid,
				)

				kv6 := kv{}
				kv6.key = k6
				kv6.value = v6
				kvs = append(kvs, kv6)
			}
		}

		if snap_count > 0 {
			snap_count = snap_count - 1
		}

		q <- kvs
	}
	kvs = []kv{}
	kvs = append(kvs, kv{key: "END"})
	q <- kvs
}

func putFunc(cmd *cobra.Command, args []string) {
	clusterid := "/openebs.io/mayastor/apis/v0/clusters/" + uuid.New().String()

	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}

	requests := make(chan v3.Op, totalClients)
	if putRate == 0 {
		putRate = math.MaxInt32
	}
	limit := rate.NewLimiter(rate.Limit(putRate), 1)
	clients := mustCreateClients(totalClients, totalConns)
	k, v := make([]byte, keySize), string(mustRandBytes(valSize))

	kt := putTotal
	if mayastor {
		kt = msnodes + msdisks + (putTotal * 3) + (putTotal * noofrepls) + mssnaps + (noofsnaps * mssnaps)

		go populate_kvs(clusterid)
		time.Sleep(2 * time.Second)
	}

	bar = pb.New(kt)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()
	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()
			for op := range requests {
				limit.Wait(context.Background())

				st := time.Now()
				_, err := c.Do(context.Background(), op)
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	var writer *bufio.Writer
	if saveKeysToFile != "" {
		fn, err := os.OpenFile(saveKeysToFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Printf("Error while opening file to save keys: %s", err.Error())
			os.Exit(1)
		}
		writer = bufio.NewWriter(fn)
		defer fn.Close()
		fmt.Print("Writer configured\n")
	}
	print_once := true
	printed_data := ""
	total_keys := 0

	if mayastor {
		go func() {
			third_kv := 3
			for kvs := range q {
				third_kv = third_kv - 1
				if print_once {
					if third_kv == 0 {
						print_once = false
						printed_data = printed_data + fmt.Sprintln("KVs :")
						printed_data = printed_data + fmt.Sprintln("---")
						for _, kv := range kvs {
							printed_data = printed_data + fmt.Sprintf("Key: %s, \nValue: (%d) \n---\n", kv.key, len(kv.value))
						}
						printed_data = printed_data + fmt.Sprintln("---")
					}
				}
				total_keys = total_keys + len(kvs)
				for _, kv := range kvs {
					if kv.key == "END" {
						close(requests)
						if writer != nil {
							writer.Flush()
						}
						return
					}
					if writer != nil {
						if _, err := writer.WriteString(kv.key + "\n"); err != nil {
							fmt.Printf("Failed to write key : %s", err.Error())
							os.Exit(1)
						}
					}
					requests <- v3.OpPut(kv.key, kv.value)
				}
			}
			close(requests)
		}()
	} else {
		go func() {
			for i := 0; i < putTotal; i++ {
				if seqKeys {
					binary.PutVarint(k, int64(i%keySpaceSize))
				} else {
					binary.PutVarint(k, int64(rand.Intn(keySpaceSize)))
				}
				if writer != nil {
					if _, err := writer.WriteString(string(k) + "\n"); err != nil {
						fmt.Printf("Failed to write key : %s", err.Error())
						os.Exit(1)
					}
				}
				requests <- v3.OpPut(string(k), v)
			}
			close(requests)
		}()
	}
	if writer != nil {
		writer.Flush()
	}

	if compactInterval > 0 {
		go func() {
			for {
				time.Sleep(compactInterval)
				compactKV(clients)
			}
		}()
	}

	rc := r.Run()
	wg.Wait()
	close(r.Results())
	bar.Finish()
	fmt.Println(<-rc)

	if checkHashkv {
		hashKV(cmd, clients)
	}

	fmt.Printf("Cluster ID : %s\n", clusterid)
	fmt.Printf("Sample data: %s\n", printed_data)
	fmt.Printf(
		"Total keys: %d, calculated: %d {msnodes: %d, msdisks: %d, puttotal: %d, mssnaps: %d}\n",
		total_keys, kt, msnodes, msdisks, putTotal, mssnaps,
	)
}

func compactKV(clients []*v3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := clients[0].KV.Get(ctx, "foo")
	cancel()
	if err != nil {
		panic(err)
	}
	revToCompact := max(0, resp.Header.Revision-compactIndexDelta)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = clients[0].KV.Compact(ctx, revToCompact)
	cancel()
	if err != nil {
		panic(err)
	}
}

func max(n1, n2 int64) int64 {
	if n1 > n2 {
		return n1
	}
	return n2
}

func hashKV(cmd *cobra.Command, clients []*v3.Client) {
	eps, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		panic(err)
	}
	for i, ip := range eps {
		eps[i] = strings.TrimSpace(ip)
	}
	host := eps[0]

	st := time.Now()
	clients[0].HashKV(context.Background(), eps[0], 0)
	rh, eh := clients[0].HashKV(context.Background(), host, 0)
	if eh != nil {
		fmt.Fprintf(os.Stderr, "Failed to get the hashkv of endpoint %s (%v)\n", host, eh)
		panic(err)
	}
	rt, es := clients[0].Status(context.Background(), host)
	if es != nil {
		fmt.Fprintf(os.Stderr, "Failed to get the status of endpoint %s (%v)\n", host, es)
		panic(err)
	}

	rs := "HashKV Summary:\n"
	rs += fmt.Sprintf("\tHashKV: %d\n", rh.Hash)
	rs += fmt.Sprintf("\tEndpoint: %s\n", host)
	rs += fmt.Sprintf("\tTime taken to get hashkv: %v\n", time.Since(st))
	rs += fmt.Sprintf("\tDB size: %s", humanize.Bytes(uint64(rt.DbSize)))
	fmt.Println(rs)
}
