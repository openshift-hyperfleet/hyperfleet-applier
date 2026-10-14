package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/serial"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

func TestLeadershipLossCancelsInFlightWrites(t *testing.T) {
	const partition = "test"
	for _, typ := range []desire.DesireType{desire.TypeApply, desire.TypeDelete} {
		t.Run(string(typ), func(t *testing.T) {
			requestStarted := make(chan struct{})
			requestCanceled := make(chan struct{})
			var startedOnce, canceledOnce sync.Once
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_, _ = io.Copy(io.Discard, req.Body)
				if req.Method == http.MethodGet {
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"target","namespace":"default"}}`)
					return
				}
				writes.Add(1)
				startedOnce.Do(func() { close(requestStarted) })
				// A real HTTP request must be canceled by the leadership context,
				// not merely by the controller's 30-second timeout.
				select {
				case <-req.Context().Done():
					canceledOnce.Do(func() { close(requestCanceled) })
				case <-t.Context().Done():
				case <-time.After(10 * time.Second):
				}
			}))
			t.Cleanup(server.Close)
			dyn, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
			mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: configMapKind}, meta.RESTScopeNamespace)
			store := memory.New()
			for _, name := range []string{"target-a", "target-b"} {
				id := desire.Identity{ManagementCluster: partition, Type: typ, Resource: configMapResource,
					Namespace: testNamespace, Name: name}
				if typ == desire.TypeApply {
					content := json.RawMessage(fmt.Sprintf(
						`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":%q,"namespace":"default"}}`, name))
					_, err = store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
						Identity: id, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content}})
				} else {
					_, err = store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{Identity: id, Owner: testOwner})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			client := fake.NewClientset()
			var fail atomic.Bool
			client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
				if fail.Load() {
					return true, nil, errors.New("Lease API unavailable")
				}
				return false, nil, nil
			})
			loop := serial.New(time.Minute,
				applydesire.New(store, store, dyn, mapper, partition),
				deletedesire.New(store, store, dyn, mapper, partition))
			r, err := New(client.CoordinationV1(), testNamespace, partition, "replica", loop)
			if err != nil {
				t.Fatal(err)
			}
			fastElection(r)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			done := make(chan error, 1)
			go func() { done <- r.Start(ctx) }()
			awaitSignal(t, requestStarted)
			fail.Store(true)
			awaitSignal(t, requestCanceled)
			select {
			case err := <-done:
				if !errors.Is(err, ErrLeadershipLost) {
					t.Fatalf("Start = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("former leader did not stop")
			}
			if writes.Load() != 1 {
				t.Fatalf("former leader issued subsequent writes: %d", writes.Load())
			}
		})
	}
}
