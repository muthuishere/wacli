package app

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestLiveIncomingUnarchivesAndSurvivesArchiveReplay(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()
	ctx := context.Background()
	chat := types.NewJID("15550000001", types.DefaultUserServer)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic", base); err != nil {
		t.Fatal(err)
	}
	setting := func(enabled bool, ts time.Time) {
		a.handleAppStatePersistenceEvent(ctx, &events.UnarchiveChatsSetting{
			Timestamp: ts,
			Action:    &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(enabled)},
		}, nil)
	}
	archive := func(ts time.Time) {
		a.handleAppStatePersistenceEvent(ctx, &events.Archive{
			JID: chat, Timestamp: ts, FromFullSync: true,
			Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)},
		}, nil)
	}
	assertArchived := func(want bool) {
		t.Helper()
		got, err := a.db.GetChat(chat.String())
		if err != nil || got.Archived != want {
			t.Fatalf("archived = %v, want %v (error %v)", got.Archived, want, err)
		}
	}
	var stored atomic.Int64
	message := func(id string, ts time.Time, own bool) {
		a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Chat: chat, Sender: chat, IsFromMe: own},
				ID:            id, Timestamp: ts,
			},
			Message: &waE2E.Message{Conversation: proto.String("synthetic arrival")},
		}, &stored, nil, nil)
	}
	setting(true, base)
	archive(base.Add(time.Second))
	message("new", base.Add(2*time.Second), false)
	assertArchived(false)
	archive(base.Add(time.Second))
	assertArchived(false)
	// A later explicit archive wins, and duplicate/own messages cannot undo it.
	archive(base.Add(3 * time.Second))
	message("new", base.Add(2*time.Second), false)
	message("own", base.Add(4*time.Second), true)
	assertArchived(true)
	// Keep-chats-archived wins even when an older preference is replayed.
	setting(false, base.Add(5*time.Second))
	setting(true, base)
	message("kept", base.Add(6*time.Second), false)
	assertArchived(true)
	setting(true, base.Add(7*time.Second))
	assertArchived(true)
	message("allowed", base.Add(8*time.Second), false)
	assertArchived(false)
	setting(false, base.Add(9*time.Second))
	archive(base.Add(3 * time.Second))
	assertArchived(false)
}

func TestAutoUnarchiveRecoveryCompletesWithoutBrokenFullReplay(t *testing.T) {
	a := newTestApp(t)
	f := &appStateContextWA{fakeWA: newFakeWA()}
	a.wa = f
	ctx := context.Background()
	chat := types.NewJID("15550000003", types.DefaultUserServer)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic", base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.SetChatArchived(chat.String(), true); err != nil {
		t.Fatal(err)
	}
	a.setAppStateFullSyncResult(appstate.WAPatchRegularLow, true)
	var messagesStored atomic.Int64
	a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{
		Info:    types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "pending", Timestamp: base.Add(2 * time.Second)},
		Message: &waE2E.Message{Conversation: proto.String("synthetic")},
	}, &messagesStored, nil, nil)
	requests := 0
	incremental := 0
	f.requestAppStateRecovery = func(context.Context, string) (types.MessageID, error) { requests++; return "snapshot", nil }
	f.fetchEvents = func(_ context.Context, name string, full, _ bool) ([]any, error) {
		if name == string(appstate.WAPatchRegularLow) && full {
			t.Error("destructive full replay on known-broken collection")
		}
		if name == string(appstate.WAPatchRegularLow) && !full {
			incremental++
		}
		return nil, nil
	}
	f.fetchAppState = func(ctx context.Context, name string, full, onlyIfNotSynced bool) error {
		_, err := f.fetchEvents(ctx, name, full, onlyIfNotSynced)
		return err
	}
	var recovering sync.Map
	a.syncAppStateDeltas(ctx, &recovering)
	if requests != 1 {
		t.Fatalf("snapshot requests=%d, want 1", requests)
	}
	if incremental != 1 {
		t.Fatalf("incremental catch-ups=%d, want 1 while snapshot pending", incremental)
	}
	if required, err := a.db.AppStateRecoveryRequired(string(appstate.WAPatchRegularLow)); err != nil || !required {
		t.Fatalf("incremental incorrectly cleared snapshot debt: %v, %v", required, err)
	}
	f.emit(&events.UnarchiveChatsSetting{Timestamp: base, Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(true)}, FromFullSync: true})
	f.emit(&events.Archive{JID: chat, Timestamp: base.Add(time.Second), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}, FromFullSync: true})
	f.emit(&events.AppStateSyncComplete{Name: appstate.WAPatchRegularLow, Recovery: true})
	waitForCondition(t, time.Second, func() bool {
		pending, err := a.db.AppStateRecoveryRequired(string(appstate.WAPatchRegularLow))
		return err == nil && !pending
	})
	got, err := a.db.GetChat(chat.String())
	if err != nil || got.Archived {
		t.Fatalf("recovered arrival=%+v, %v", got, err)
	}
	var next sync.Map
	a.syncAppStateDeltas(ctx, &next)
	if requests != 1 {
		t.Fatalf("completed snapshot requested again: %d", requests)
	}
}

func TestAutoUnarchiveWaitsForPreferenceAndArchiveBoundary(t *testing.T) {
	for _, preferenceFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "archive first", true: "preference first"}[preferenceFirst], func(t *testing.T) {
			a := newTestApp(t)
			a.wa = newFakeWA()
			ctx := context.Background()
			chat := types.NewJID("15550000002", types.DefaultUserServer)
			base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic", base); err != nil {
				t.Fatal(err)
			}
			if err := a.db.SetChatArchived(chat.String(), true); err != nil {
				t.Fatal(err)
			}
			var stored atomic.Int64
			a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{
				Info:    types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "pending", Timestamp: base.Add(2 * time.Second)},
				Message: &waE2E.Message{Conversation: proto.String("pending preference recovery")},
			}, &stored, nil, nil)
			assert := func(want bool) {
				t.Helper()
				got, err := a.db.GetChat(chat.String())
				if err != nil || got.Archived != want {
					t.Fatalf("archived=%v, want %v; %v", got.Archived, want, err)
				}
			}
			assert(true)
			pref := &events.UnarchiveChatsSetting{Timestamp: base, Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(true)}, FromFullSync: true}
			arch := &events.Archive{JID: chat, Timestamp: base.Add(time.Second), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}, FromFullSync: true}
			first, second := any(arch), any(pref)
			if preferenceFirst {
				first, second = pref, arch
			}
			a.handleAppStatePersistenceEvent(ctx, first, nil)
			assert(true)
			a.handleAppStatePersistenceEvent(ctx, second, nil)
			assert(false)
			// Shared history ingestion must not act as a fresh arrival.
			a.handleAppStatePersistenceEvent(ctx, &events.Archive{JID: chat, Timestamp: base.Add(3 * time.Second), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}}, nil)
			if err := a.storeParsedMessage(ctx, wa.ParsedMessage{Chat: chat, ID: "history", Timestamp: base.Add(4 * time.Second), Text: "history"}, messageStoreOptions{}); err != nil {
				t.Fatal(err)
			}
			assert(true)
		})
	}
}
