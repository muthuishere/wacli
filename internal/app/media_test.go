package app

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/store"
)

func TestMediaEnqueuerBackpressure(t *testing.T) {
	for _, cancelBlocked := range []bool{false, true} {
		name := "capacity"
		if cancelBlocked {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			queue := newMediaQueue(1)
			enqueue := newMediaEnqueuer(ctx, queue)
			enqueue("chat", "first")

			started, returned := make(chan struct{}), make(chan struct{})
			go func() {
				close(started)
				enqueue("chat", "second")
				close(returned)
			}()
			<-started
			// A full queue must block its caller, not spawn an unbounded
			// enqueue goroutine. Process-wide counts include unrelated work.
			select {
			case <-returned:
				t.Fatal("enqueue returned while the media queue was full")
			case <-time.After(50 * time.Millisecond):
			}
			if cancelBlocked {
				cancel()
			} else {
				first := <-queue.jobs
				queue.done()
				if first.msgID != "first" {
					t.Fatalf("first job = %q", first.msgID)
				}
			}
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("enqueue stayed blocked after capacity or cancellation")
			}
			if !cancelBlocked {
				second := <-queue.jobs
				queue.done()
				if second.msgID != "second" {
					t.Fatalf("second job = %q", second.msgID)
				}
			}
		})
	}
}

func TestMediaQueueWaitIdleFencesActiveProducer(t *testing.T) {
	queue := newMediaQueue(1)
	if !queue.beginProducer() {
		t.Fatal("expected producer admission before drain")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drained := make(chan bool, 1)
	go func() {
		drained <- queue.waitIdle(ctx)
	}()

	select {
	case <-drained:
		t.Fatal("queue reported idle while an event producer was active")
	case <-time.After(50 * time.Millisecond):
	}

	queue.endProducer()
	select {
	case ok := <-drained:
		if !ok {
			t.Fatal("queue drain was canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("queue did not drain after producer finished")
	}
	if queue.beginProducer() {
		t.Fatal("queue admitted a producer after the drain fence")
	}
}

func TestDownloadMediaJobMarksDownloaded(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := "123@s.whatsapp.net"
	if err := a.db.UpsertChat(chat, "dm", "Alice", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(store.UpsertMessageParams{
		ChatJID:       chat,
		MsgID:         "mid",
		SenderJID:     chat,
		SenderName:    "Alice",
		Timestamp:     time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		FromMe:        false,
		Text:          "",
		MediaType:     "image",
		MediaCaption:  "cap",
		Filename:      "pic.jpg",
		MimeType:      "image/jpeg",
		DirectPath:    "/direct/path",
		MediaKey:      []byte{1, 2, 3},
		FileSHA256:    []byte{4, 5},
		FileEncSHA256: []byte{6, 7},
		FileLength:    123,
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	downloaded, err := a.downloadMediaJob(context.Background(), mediaJob{chatJID: chat, msgID: "mid"})
	if err != nil {
		t.Fatalf("downloadMediaJob: %v", err)
	}
	if !downloaded {
		t.Fatalf("expected downloaded=true")
	}

	info, err := a.db.GetMediaDownloadInfo(chat, "mid")
	if err != nil {
		t.Fatalf("GetMediaDownloadInfo: %v", err)
	}
	if info.LocalPath == "" {
		t.Fatalf("expected LocalPath to be set")
	}
	if _, err := os.Stat(info.LocalPath); err != nil {
		t.Fatalf("expected downloaded file to exist: %v", err)
	}
}
