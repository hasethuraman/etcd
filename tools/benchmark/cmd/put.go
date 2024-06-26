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
	"encoding/json"
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

type NodeSpec struct {
	Id            string            `json:"id"`
	Endpoint      string            `json:"endpoint"`
	Labels        map[string]string `json:"labels"`
	Cordon_labels []string          `json:"cordon_labels"`
}

type PoolSpec struct {
	Node          string            `json:"node"`
	Id            string            `json:"id"`
	Disks         []string          `json:"disks"`
	Status        map[string]string `json:"status"`
	Labels        map[string]string `json:"labels"`
	Cordon_labels []string          `json:"cordon_labels"`
	Operation     *string           `json:"operation"`
	PoolType      string            `json:"pooltype"`
	Revision      int               `json:"revision"`
}

type VolumeSpec struct {
	Uuid          string                                             `json:"uuid"`
	Size          int64                                              `json:"size"`
	Labels        map[string]string                                  `json:"labels"`
	Num_replicas  int                                                `json:"num_replicas"`
	Status        map[string]string                                  `json:"status"`
	Target        map[string]string                                  `json:"target"`
	Policy        map[string]string                                  `json:"policy"`
	Topology      map[string]map[string]map[string]map[string]string `json:"topology"`
	Last_nexus_id string                                             `json:"last_nexus_id"`
	Operation     *string                                            `json:"operation"`
	Thin          bool                                               `json:"thin"`
	Revision      int                                                `json:"revision"`
}

type ReplicaSpec struct {
	Name      string            `json:"name"`
	Uuid      string            `json:"uuid"`
	Size      int64             `json:"size"`
	Pool      string            `json:"pool"`
	Share     string            `json:"share"`
	Thin      bool              `json:"thin"`
	Status    map[string]string `json:"status"`
	Managed   bool              `json:"managed"`
	Owners    map[string]string `json:"owners"`
	Operation *string           `json:"operation"`
	Revision  int               `json:"revision"`
}

type NexusChild struct {
	Replica Replica `json:"Replica"`
}

type Replica struct {
	Uuid      string `json:"uuid"`
	Share_uri string `json:"share_uri"`
}

type NexusSpec struct {
	Uuid          string            `json:"uuid"`
	Name          string            `json:"name"`
	Node          string            `json:"node"`
	Children      []NexusChild      `json:"children"`
	Size          int64             `json:"size"`
	Spec_status   map[string]string `json:"spec_status"`
	Share         string            `json:"share"`
	Managed       bool              `json:"managed"`
	Owner         string            `json:"owner"`
	Operation     *string           `json:"operation"`
	Revision      int               `json:"revision"`
	Last_snapshot string            `json:"last_snapshot"`
}

type VolumeNexusChildInfo struct {
	Healthy     bool   `json:"healthy"`
	Uuid        string `json:"uuid"`
	Rebuild_map []byte `json:"rebuild_map"`
}

type VolumeNexusInfo struct {
	Children       []VolumeNexusChildInfo `json:"children"`
	Clean_shutdown bool                   `json:"clean_shutdown"`
}

type VolumeSnapshotSpec struct {
	Name               string            `json:"name"`
	Volume_id          string            `json:"volume_id"`
	Nexus_id           string            `json:"nexus_id"`
	Creation_timestamp string            `json:"creation_timestamp"`
	Status             map[string]string `json:"status"`
	Operation          *string           `json:"operation"`
	Revision           int               `json:"revision"`
}

type ReplicaSnapshotSpec struct {
	Name          string            `json:"name"`
	Uuid          string            `json:"uuid"`
	Snapshot_name string            `json:"snapshot_name"`
	Source_id     string            `json:"source_id"`
	Pool_id       string            `json:"pool_id"`
	Owner         map[string]string `json:"owner"`
	Status        map[string]string `json:"status"`
	Operation     *string           `json:"operation"`
	Revision      int               `json:"revision"`
}

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark put",

	Run: putFunc,
}

type kv struct {
	key   string
	value []byte
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

func createArrayWithSameValue[T any](length int, value T) []T {
	result := make([]T, length/8)
	for i := range result {
		result[i] = value
	}
	return result
}

func populate_kvs(clusterid string) {
	cn := uint(0)

	kvs := []kv{}
	for i := 0; i < msnodes; i++ {
		k1 := fmt.Sprintf(clusterid+"/namespaces/default/NodeSpec/k8s-agentpool1-10232180-vmss00%05d", i)
		v1, err := json.Marshal(
			NodeSpec{
				Id:       fmt.Sprintf("k8s-agentpool1-10232180-%05d", i),
				Endpoint: fmt.Sprintf("%s:10124", generateRandomIP()),
				Labels: map[string]string{
					"topology.kubernetestorage.csi.mayastor.com/zone": "",
				},
				Cordon_labels: []string{},
			},
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal node spec: %v\n", err)
			os.Exit(1)
		}
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
		v1, err := json.Marshal(
			PoolSpec{
				Node:   agentpoold,
				Id:     "csi-abcde",
				Disks:  []string{"/abc/dis"},
				Status: map[string]string{"Created": "Online"},
				Labels: map[string]string{
					"default.cloud.org/storagedisk":   "testdiskpoolfromcloud-request-xyz",
					"default.cloud.org/provisionedby": "someone-by-administring",
					"default.cloud.org/somepool":      "testdiskpoolfromcloud-request-xyz-diskpool-abcde",
					"default.cloud.org/plumber":       "somediskpool-provisioned-24gdd",
					"openebs.io/created-by":           "operator-diskpool",
					"default.cloud.org/type-of-disk":  "generallycloud",
				},
				Operation: nil,
				PoolType:  "any",
				Revision:  300,
			},
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal node spec: %v\n", err)
			os.Exit(1)
		}
		kvs = append(kvs, kv{key: k1, value: v1})
	}
	if len(kvs) > 0 {
		q <- kvs
	}

	rebuild_map_byte_array := createArrayWithSameValue(noofsegs, uint8(255))
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
		v1, err := json.Marshal(VolumeSpec{
			Uuid:         volid,
			Size:         2147483648,
			Labels:       nil,
			Num_replicas: 1,
			Status:       map[string]string{"Created": "Online"},
			Target:       map[string]string{"node": agentpoold, "nexus": nexid, "protocol": "nvmf"},
			Policy:       map[string]string{"self_heal": "true"},
			Topology: map[string]map[string]map[string]map[string]string{
				"node": nil,
				"pool": {
					"Labelled": {
						"exclusion": nil,
						"inclusion": {"openebs.io/created-by": "operator-diskpool"},
					},
				},
			},
			Last_nexus_id: nexid,
			Operation:     nil,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal volume spec: %v\n", err)
			os.Exit(1)
		}
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
			v2, err := json.Marshal(
				ReplicaSpec{
					Name:      replid,
					Uuid:      replid,
					Size:      2147483648,
					Pool:      "diskpool-ppndp",
					Share:     "none",
					Thin:      false,
					Status:    map[string]string{"Created": "online"},
					Managed:   true,
					Owners:    map[string]string{"volume": volid},
					Operation: nil,
				},
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to marshal replica spec: %v\n", err)
				os.Exit(1)
			}

			kv2 := kv{}
			kv2.key = k2
			kv2.value = v2

			kvs = append(kvs, kv2)
		}

		if rebuild_count > 0 {
			rebuild_count = rebuild_count - 1
		}
		if rebuild_count == 0 {
			rebuild_map_byte_array = nil
		}
		k3 := clusterid + "/namespaces/default/NexusSpec/" + nexid
		k4 := clusterid + "/namespaces/default/volume/" + volid + "/nexus/" + nexid + "/info"
		nexus := NexusSpec{
			Uuid:     nexid,
			Name:     volid,
			Node:     agentpoold,
			Children: nil,
			Size:     2147483648,
			Spec_status: map[string]string{
				"Created": "Online",
			},
			Share:     "nvmf",
			Managed:   true,
			Owner:     volid,
			Operation: nil,
			Revision:  0,
		}
		vol_nexus := VolumeNexusInfo{
			Children:       nil,
			Clean_shutdown: false,
		}
		var nexus_children []NexusChild
		var vol_nexus_children []VolumeNexusChildInfo
		l_rebuild_byte_array := rebuild_map_byte_array
		for _, replid := range replids {
			nexus_children = append(nexus_children, NexusChild{Replica{Uuid: replid, Share_uri: fmt.Sprintf("nvmf://10.1.0.5:8420/nqn.2019-05.io.openebs:/xfs-disk-pool/loopxyz2023070702/%s?uuid=%s", replid, replid)}})
			vol_nexus_children = append(vol_nexus_children, VolumeNexusChildInfo{Healthy: true, Uuid: replid, Rebuild_map: l_rebuild_byte_array})
			l_rebuild_byte_array = nil
		}
		nexus.Children = nexus_children
		vol_nexus.Children = vol_nexus_children

		kv3 := kv{}
		kv3.key = k3
		kv3.value, err = json.Marshal(nexus)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal nexus spec: %v\n", err)
			os.Exit(1)
		}

		kv4 := kv{}
		kv4.key = k4
		kv4.value, err = json.Marshal(vol_nexus)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal vol nexus spec: %v\n", err)
			os.Exit(1)
		}

		kvs = append(kvs, kv3)
		kvs = append(kvs, kv4)

		if snap_count == 0 {
			mssnaps = 0
		}

		if mssnaps > 0 {
			snapid := uuid.New().String()
			k5 := clusterid + "/namespaces/default/VolumeSnapshotSpec/" + volid + "@snapshot-" + snapid
			v5, err := json.Marshal(
				VolumeSnapshotSpec{
					Name:               volid + "@snapshot-" + snapid,
					Volume_id:          volid,
					Nexus_id:           nexid,
					Creation_timestamp: "2024-06-24T11:50:02.516046460Z",
					Status:             map[string]string{"Created": "null"},
					Operation:          nil,
					Revision:           200,
				},
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to marshal volume snapshot spec: %v\n", err)
				os.Exit(1)
			}
			kv5 := kv{}
			kv5.key = k5
			kv5.value = v5
			kvs = append(kvs, kv5)

			for j := 0; j < mssnaps; j++ {
				repid := uuid.New().String()
				repname := uuid.New().String()
				k6 := clusterid + "/namespaces/default/ReplicaSnapshotSpec/" + repid
				v6, err := json.Marshal(
					ReplicaSnapshotSpec{
						Name:          repname + "@snapshot-" + snapid,
						Uuid:          repid,
						Snapshot_name: volid + "@snapshot-" + snapid,
						Source_id:     repname,
						Pool_id:       volid + "@snapshot-" + snapid,
						Owner:         map[string]string{"volume_snapshot": volid + "@snapshot-" + snapid},
						Status:        map[string]string{"Created": "null"},
						Operation:     nil,
						Revision:      200,
					},
				)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to marshal replica snapshot spec: %v\n", err)
					os.Exit(1)
				}

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
					requests <- v3.OpPut(kv.key, string(kv.value))
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
