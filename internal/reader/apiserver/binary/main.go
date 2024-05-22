package main

import (
	"log"
	"net/http"
	"path/filepath"
	"time"
	"unsafe"

	reader "github.com/element-of-surprise/auditARG/internal/reader/apiserver"

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

	r, err := reader.New(informerFactory, reader.RTNode|reader.RTPod|reader.RTNamespace)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	for data := range r.Stream() {
		switch data.Type {
		case reader.DTNode:
			switch data.Node.Type {
			case reader.CTAdd:
				log.Println("Node added:\n", mustJSONMarshal(data.Node.New))
			case reader.CTUpdate:
				log.Println("Node updated:\n", mustJSONMarshal(data.Node.New))
			case reader.CTDelete:
				log.Println("Node deleted:\n", mustJSONMarshal(data.Node.Old))
			}
		case reader.DTPod:
			switch data.Pod.Type {
			case reader.CTAdd:
				log.Println("Pod added:\n", mustJSONMarshal(data.Pod.New))
			case reader.CTUpdate:
				log.Println("Pod updated:\n", mustJSONMarshal(data.Pod.New))
			case reader.CTDelete:
				log.Println("Pod deleted:\n", mustJSONMarshal(data.Pod.Old))
			}
		case reader.DTNamespace:
			switch data.Namespace.Type {
			case reader.CTAdd:
				log.Println("Namespace added:\n", mustJSONMarshal(data.Namespace.New))
			case reader.CTUpdate:
				log.Println("Namespace updated:\n", mustJSONMarshal(data.Namespace.New))
			case reader.CTDelete:
				log.Println("Namespace deleted:\n", mustJSONMarshal(data.Namespace.Old))
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
