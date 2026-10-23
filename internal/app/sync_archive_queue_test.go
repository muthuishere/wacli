package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestAutoUnarchivePreservesDispatchOrderAcrossQueuedActions(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		settingQueued bool
		remapped      bool
	}{
		{name: "queued archive"},
		{name: "queued setting", settingQueued: true},
		{name: "queued archive remapped", remapped: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			settingQueued := scenario.settingQueued
			a := newTestApp(t)
			f := newFakeWA()
			a.wa = f
			ctx := context.Background()
			chat := types.NewJID("15550000009", types.DefaultUserServer)
			archiveChat := chat
			if scenario.remapped {
				archiveChat = types.NewJID("15550000009", types.HiddenUserServer)
			}
			base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic", base); err != nil {
				t.Fatal(err)
			}
			if err := a.db.SetChatArchiveSetting(!settingQueued, base.Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := a.db.ApplyChatArchive(chat.String(), true, base); err != nil {
				t.Fatal(err)
			}
			blocker := a.appStatePersist.reserve()
			if settingQueued {
				a.handleAppStatePersistenceEvent(ctx, &events.UnarchiveChatsSetting{Timestamp: base.Add(500 * time.Millisecond), Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(true)}}, nil)
			} else {
				a.handleAppStatePersistenceEvent(ctx, &events.Archive{JID: archiveChat, Timestamp: base.Add(500 * time.Millisecond), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}}, nil)
			}
			if scenario.remapped {
				f.lids[archiveChat] = chat
			}
			var stored atomic.Int64
			a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "new", Timestamp: base}, Message: &waE2E.Message{Conversation: proto.String("synthetic")}}, &stored, nil, nil)
			a.appStatePersist.completeOne(blocker, func() {})
			if err := a.appStatePersist.waitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := a.db.GetChat(chat.String())
			if err != nil || got.Archived {
				t.Fatalf("earlier queued action overrode later incoming: %+v, %v", got, err)
			}
		})
	}
}

func TestAutoUnarchiveQueuedDisableDoesNotUseOldEnabledSetting(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()
	ctx := context.Background()
	chat := types.NewJID("15550000010", types.DefaultUserServer)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic", base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.SetChatArchiveSetting(true, base.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := a.db.ApplyChatArchive(chat.String(), true, base); err != nil {
		t.Fatal(err)
	}
	blocker := a.appStatePersist.reserve()
	a.handleAppStatePersistenceEvent(ctx, &events.Archive{JID: chat, Timestamp: base.Add(500 * time.Millisecond), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}}, nil)
	a.handleAppStatePersistenceEvent(ctx, &events.UnarchiveChatsSetting{Timestamp: base.Add(600 * time.Millisecond), Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(false)}}, nil)
	var stored atomic.Int64
	a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "new", Timestamp: base}, Message: &waE2E.Message{Conversation: proto.String("synthetic")}}, &stored, nil, nil)
	// Release even when the pre-drain assertion fails so cleanup can drain.
	released := false
	defer func() {
		if !released {
			a.appStatePersist.completeOne(blocker, func() {})
			_ = a.appStatePersist.waitIdle(ctx)
		}
	}()
	got, err := a.db.GetChat(chat.String())
	if err != nil || !got.Archived {
		t.Fatalf("pending disable used stale enabled preference: %+v, %v", got, err)
	}
	a.appStatePersist.completeOne(blocker, func() {})
	released = true
	if err := a.appStatePersist.waitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = a.db.GetChat(chat.String())
	if err != nil || !got.Archived {
		t.Fatalf("disabled preference unarchived chat: %+v, %v", got, err)
	}
}

func TestAutoUnarchiveEqualTimestampDisableRemainsPending(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()
	ctx := context.Background()
	chat := types.NewJID("15550000014", types.DefaultUserServer)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := a.db.ApplyChatArchive(chat.String(), true, base); err != nil {
		t.Fatal(err)
	}
	first := &events.UnarchiveChatsSetting{Timestamp: base.Add(600 * time.Millisecond), Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(true)}}
	last := &events.UnarchiveChatsSetting{Timestamp: first.Timestamp, Action: &waSyncAction.UnarchiveChatsSetting{UnarchiveChats: proto.Bool(false)}}
	a.observeArchiveEvent(ctx, first, false)
	a.observeArchiveEvent(ctx, last, false)
	if err := a.persistAppStateEvent(ctx, first, nil); err != nil {
		t.Fatal(err)
	}
	var stored atomic.Int64
	a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "new", Timestamp: base}, Message: &waE2E.Message{Conversation: proto.String("synthetic")}}, &stored, nil, nil)
	if err := a.persistAppStateEvent(ctx, last, nil); err != nil {
		t.Fatal(err)
	}
	got, err := a.db.GetChat(chat.String())
	if err != nil || !got.Archived {
		t.Fatalf("earlier equal-timestamp enable hid pending disable: %+v, %v", got, err)
	}
}

func TestAutoUnarchiveIncomingDuringFetch(t *testing.T) {
	for _, laterArchive := range []bool{false, true} {
		t.Run(map[bool]string{false: "incoming wins", true: "later archive wins"}[laterArchive], func(t *testing.T) {
			a := newTestApp(t)
			f := &appStateContextWA{fakeWA: newFakeWA()}
			a.wa = f
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			chat := types.NewJID("15550000015", types.DefaultUserServer)
			base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			if err := a.db.SetChatArchiveSetting(true, base.Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := a.db.ApplyChatArchive(chat.String(), true, base); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			f.fetchEvents = func(ctx context.Context, _ string, _, _ bool) ([]any, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return []any{&events.Archive{JID: chat, Timestamp: base.Add(500 * time.Millisecond), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}}}, nil
			}
			done := make(chan error, 1)
			go func() {
				fetchErr, persistErr := a.fetchAndPersistAppState(ctx, appstate.WAPatchRegularLow, true, &appStatePersistenceTracker{})
				if fetchErr != nil {
					done <- fetchErr
				} else {
					done <- persistErr
				}
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var stored atomic.Int64
			a.handleLiveSyncMessage(ctx, SyncOptions{}, &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "during-fetch", Timestamp: base}, Message: &waE2E.Message{Conversation: proto.String("synthetic")}}, &stored, nil, nil)
			if stored.Load() != 1 {
				t.Fatal("message storage waited for the app-state fetch")
			}
			if laterArchive {
				a.handleAppStatePersistenceEvent(ctx, &events.Archive{JID: chat, Timestamp: base.Add(700 * time.Millisecond), Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}}, nil)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := a.appStatePersist.waitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := a.db.GetChat(chat.String())
			if err != nil || got.Archived != laterArchive {
				t.Fatalf("fetch ordering lost: %+v, %v", got, err)
			}
		})
	}
}
