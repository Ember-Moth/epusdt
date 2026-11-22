package telegram

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func withTelegramEnv(token, proxy string, manage int64, fn func()) {
	origToken := config.TgBotToken
	origProxy := config.TgProxy
	origManage := config.TgManage
	defer func() {
		config.TgBotToken = origToken
		config.TgProxy = origProxy
		config.TgManage = origManage
	}()

	config.TgBotToken = token
	config.TgProxy = proxy
	config.TgManage = manage
	fn()
}

func TestLoadCommandBotConfig_DoesNotUseEnvFallbackWhenNoChannel(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	// Env config is present but settings table is empty —
	// the bot must NOT fall back to config.TgBotToken.
	withTelegramEnv("env-token", "http://127.0.0.1:7890", 123456789, func() {
		cfg, source, err := loadCommandBotConfig()
		if err != nil {
			t.Fatalf("loadCommandBotConfig err: %v", err)
		}
		if cfg != nil {
			t.Fatalf("expected nil config when settings table has no telegram keys, got %+v", cfg)
		}
		if source != "" {
			t.Fatalf("source = %q, want empty", source)
		}
	})
}

func TestLoadCommandBotConfig_PrefersEnabledTelegramChannel(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	// Seed settings table with telegram credentials.
	dao.Mdb.Create(&mdb.Setting{Group: "system", Key: "system.telegram_bot_token", Value: "db-token", Type: "string"})
	dao.Mdb.Create(&mdb.Setting{Group: "system", Key: "system.telegram_chat_id", Value: "987654321", Type: "string"})
	_ = data.ReloadSettings()

	withTelegramEnv("env-token", "", 123456789, func() {
		cfg, source, err := loadCommandBotConfig()
		if err != nil {
			t.Fatalf("loadCommandBotConfig err: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil config")
		}
		if source != "settings" {
			t.Fatalf("source = %q, want %q", source, "settings")
		}
		if cfg.BotToken != "db-token" {
			t.Fatalf("bot token = %q, want %q", cfg.BotToken, "db-token")
		}
		if cfg.ChatID != 987654321 {
			t.Fatalf("chat id = %d, want %d", cfg.ChatID, int64(987654321))
		}
	})
}

func TestLoadCommandBotConfig_ReturnsNilWhenNoChannelAndNoEnv(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	// Fresh DB — ensure the cache doesn't carry over values from a previous test.
	_ = data.ReloadSettings()

	withTelegramEnv("", "", 0, func() {
		cfg, source, err := loadCommandBotConfig()
		if err != nil {
			t.Fatalf("loadCommandBotConfig err: %v", err)
		}
		if cfg != nil {
			t.Fatalf("expected nil config, got %+v", cfg)
		}
		if source != "" {
			t.Fatalf("source = %q, want empty", source)
		}
	})
}

func TestLoadCommandBotConfig_SkipsEmptyConfigChannel(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	// Only chat_id present, no bot_token → should return nil.
	dao.Mdb.Create(&mdb.Setting{Group: "system", Key: "system.telegram_chat_id", Value: "987654321", Type: "string"})
	_ = data.ReloadSettings()

	cfg, source, err := loadCommandBotConfig()
	if err != nil {
		t.Fatalf("loadCommandBotConfig err: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config when bot_token is absent, got %+v", cfg)
	}
	if source != "" {
		t.Fatalf("source = %q, want empty", source)
	}
}

func TestLoadCommandBotConfig_SkipsInvalidAndUsesNextValid(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	// bot_token present but chat_id is not a valid int → should return error.
	dao.Mdb.Create(&mdb.Setting{Group: "system", Key: "system.telegram_bot_token", Value: "db-token", Type: "string"})
	dao.Mdb.Create(&mdb.Setting{Group: "system", Key: "system.telegram_chat_id", Value: "not-a-number", Type: "string"})
	_ = data.ReloadSettings()

	cfg, _, err := loadCommandBotConfig()
	if err == nil {
		t.Fatal("expected error for invalid chat_id, got nil")
	}
	if cfg != nil {
		t.Fatalf("expected nil config on parse error, got %+v", cfg)
	}
}

// TestWaitPendingReloads_WaitsForAsyncReload guards the hook tests use to
// stop ReloadBotAsync goroutines from outliving the test that started them
// (they read log.Sugar and dao.Mdb, which the next test reassigns).
func TestWaitPendingReloads_WaitsForAsyncReload(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()
	_ = data.ReloadSettings() // no telegram settings → reload logs "disabled" and returns

	core, logs := observer.New(zapcore.InfoLevel)
	prevSugar := log.Sugar
	log.Sugar = zap.New(core).Sugar()
	t.Cleanup(func() { log.Sugar = prevSugar })

	// Hold reloadMu so the reload goroutine cannot finish before we wait.
	reloadMu.Lock()
	ReloadBotAsync("test")
	time.AfterFunc(50*time.Millisecond, reloadMu.Unlock)

	WaitPendingReloads()

	if got := logs.FilterMessageSnippet("command bot disabled").Len(); got != 1 {
		t.Fatalf("expected the reload to have finished and logged once, got %d entries: %v", got, logs.All())
	}
}
