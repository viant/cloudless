# Cloud resource utility



### Asset change tracker

Asset tracker keep watching the watch URL for any change.
In case modification takes place a callback is called with changed URL and resource.Operation

```go
package mypackage;
import (
	"github.com/viant/cloudless/resource"
	"github.com/viant/afs"
	"time"
	"context"
	"fmt"
	"log"
)

func ExampleTracker_Notify() {
	watchURL := "myProto://myBucket/myFolder"
	tracker := resource.New(watchURL, time.Second)
	fs := afs.New()
	err := tracker.Notify(context.Background(), fs, func(URL string, operation resource.Operation) {
		switch operation {
		case resource.Added:
			fmt.Printf("addd :%v", URL)
		case resource.Modified:
			fmt.Printf("addd :%v", URL)
		case resource.Deleted:
			fmt.Printf("addd :%v", URL)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}

```
