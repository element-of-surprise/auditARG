package main

import (
	"context"
	"log"
	"net/http"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/element-of-surprise/auditARG/internal/batching"
	ireader "github.com/element-of-surprise/auditARG/internal/readers/apiserver/informers"
	"github.com/element-of-surprise/auditARG/internal/readers/data"
	"github.com/element-of-surprise/auditARG/internal/readers/safety"
	"github.com/element-of-surprise/auditARG/internal/routing"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	_ "net/http/pprof"

	_ "go.uber.org/automaxprocs"
)

func main() {
	bkCtx := context.Background()

	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	} else {
		kubeconfig = ""
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	informerFactory := informers.NewSharedInformerFactory(clientset, 5*time.Minute)

	r, err := ireader.New(informerFactory, ireader.RTNode|ireader.RTPod|ireader.RTNamespace)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	secretsOut := make(chan data.Entry, 1)
	_, err = safety.New(r.Stream(), secretsOut)
	if err != nil {
		panic(err)
	}

	batcherOut := make(chan batching.Batches, 1)
	_, err = batching.New(secretsOut, batcherOut, 5*time.Second)
	if err != nil {
		panic(err)
	}

	router, err := routing.New(batcherOut)
	if err != nil {
		panic(err)
	}

	logInformersIn := make(chan batching.Batches, 1)

	router.Register(bkCtx, "logInformers", logInformersIn)
	router.Start(bkCtx)

	logInformers(bkCtx, logInformersIn) // blocks
}

func logInformers(ctx context.Context, in chan batching.Batches) {
	for batches := range in {
		log.Println("Received batch")
		for entry := range batches.Iter(ctx) {
			switch entry.Type {
			case data.ETInformer:
				d, err := entry.Informer()
				if err != nil {
					panic(err)
				}
				switch d.Type {
				case data.OTNode:
					n, err := d.Node()
					if err != nil {
						panic(err)
					}
					switch n.ChangeType {
					case data.CTAdd:
						log.Println("Node added:\n", mustJSONMarshal(n.New))
					case data.CTUpdate:
						log.Println("Node updated:\n", mustJSONMarshal(n.New))
					case data.CTDelete:
						log.Println("Node deleted:\n", mustJSONMarshal(n.Old))
					}
				case data.OTPod:
					p, err := d.Pod()
					if err != nil {
						panic(err)
					}
					switch p.ChangeType {
					case data.CTAdd:
						log.Println("Pod added:\n", mustJSONMarshal(p.New))
					case data.CTUpdate:
						log.Println("Pod updated:\n", mustJSONMarshal(p.New))
					case data.CTDelete:
						log.Println("Pod deleted:\n", mustJSONMarshal(p.Old))
					}
				case data.OTNamespace:
					ns, err := d.Namespace()
					if err != nil {
						panic(err)
					}
					switch ns.ChangeType {
					case data.CTAdd:
						log.Println("Namespace added:\n", mustJSONMarshal(ns.New))
					case data.CTUpdate:
						log.Println("Namespace updated:\n", mustJSONMarshal(ns.New))
					case data.CTDelete:
						log.Println("Namespace deleted:\n", mustJSONMarshal(ns.Old))
					}
				}
			}
		}
	}
}

func mustJSONMarshal(data any) string {
	b, err := json.Marshal(data, json.DefaultOptionsV2())
	if err != nil {
		panic(err)
	}
	(*jsontext.Value)(&b).Indent("", "\t")

	return unsafe.String(unsafe.SliceData(b), len(b))
}
