// Command docproc-server runs docproc as an HTTP service.
//
// Configuration comes from environment variables (see the README):
//
//	HTTP_ADDR          listen address, default :8080
//	DATABASE_URL       PostgreSQL DSN; empty = in-memory repository
//	PRINCIPAL_URL      Principal base URL; empty = local simulator
//	CALLBACK_SECRET    HMAC secret for verifying callbacks; required with PRINCIPAL_URL
//	SUBMIT_TIMEOUT     timeout per request to the Principal, default 30s
//	LEASE_TIMEOUT      when a PROCESSING document counts as stuck, default 2m
//	RECOVERY_INTERVAL  recovery job interval, default 30s
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/docproc/memory"
	"github.com/sabiqsa/go-document-processor/docproc/postgres"
	"github.com/sabiqsa/go-document-processor/httpapi"
	"github.com/sabiqsa/go-document-processor/internal/simulator"
	"github.com/sabiqsa/go-document-processor/principal"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Root ctx: cancelled on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, closeRepo, err := openRepository(cfg)
	if err != nil {
		return err
	}
	defer closeRepo()

	var (
		client docproc.PrincipalClient
		sim    *simulator.Principal
	)
	if cfg.principalURL != "" {
		client = principal.NewClient(cfg.principalURL, nil)
	} else {
		log.Warn("PRINCIPAL_URL is empty, using the simulated Principal")
		sim = simulator.New(simulator.Config{RejectRate: 0.15, FailRate: 0.2, DuplicateRate: 0.2, MaxDelay: 300 * time.Millisecond}, log)
		client = sim
	}
	if cfg.callbackSecret == "" {
		// loadConfig only allows this together with the simulator.
		log.Warn("CALLBACK_SECRET is empty, callback signatures are NOT verified")
	}

	svc := docproc.NewService(repo, client,
		docproc.WithLogger(log),
		docproc.WithSubmitTimeout(cfg.submitTimeout),
		docproc.WithLeaseTimeout(cfg.leaseTimeout),
		docproc.WithBackoff(docproc.ExponentialBackoff(5*time.Second, 5*time.Minute)),
	)

	// Background work has its own ctx so the shutdown order can be
	// controlled: HTTP stops accepting requests first, then the work stops.
	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	runner := httpapi.NewRunner(workCtx, log)

	var bg sync.WaitGroup
	if sim != nil {
		sim.SetCallback(svc.HandleCallback)
		bg.Go(func() { sim.Run(workCtx) })
	}
	bg.Go(func() { recoveryLoop(workCtx, svc, cfg.recoveryInterval, log) })

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           httpapi.New(svc, runner, httpapi.Config{CallbackSecret: cfg.callbackSecret, Logger: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// Graceful shutdown:
	// 1. stop accepting new requests and wait for in-flight ones to finish
	// 2. cancel ProcessBatch and the background loops. Documents in Submit
	//    are returned to PENDING by ProcessDocument; anything that slips
	//    through (e.g. the process is killed) is picked up by RecoverStale
	//    on the next start.
	// 3. wait for every goroutine, then close the database (defer)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	stopWork()
	runner.Wait()
	bg.Wait()
	log.Info("shutdown complete", "stats", svc.Stats())
	return nil
}

func recoveryLoop(ctx context.Context, svc *docproc.Service, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		// Run once right at startup to clean up after a previous crash.
		if _, err := svc.RecoverStale(ctx); err != nil && ctx.Err() == nil {
			log.Error("recovery failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func openRepository(cfg config) (docproc.Repository, func(), error) {
	if cfg.databaseURL == "" {
		return memory.New(), func() {}, nil
	}
	db, err := postgres.Open(cfg.databaseURL, postgres.PoolConfig{
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: 30 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		sqlDB.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	return postgres.New(db), func() { sqlDB.Close() }, nil
}

type config struct {
	addr             string
	databaseURL      string
	principalURL     string
	callbackSecret   string
	submitTimeout    time.Duration
	leaseTimeout     time.Duration
	recoveryInterval time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		addr:           envOr("HTTP_ADDR", ":8080"),
		databaseURL:    os.Getenv("DATABASE_URL"),
		principalURL:   os.Getenv("PRINCIPAL_URL"),
		callbackSecret: os.Getenv("CALLBACK_SECRET"),
	}
	// Against a real Principal, an unsigned callback endpoint would let
	// anyone mark documents SUCCESS. Fail closed instead of starting.
	if cfg.principalURL != "" && cfg.callbackSecret == "" {
		return cfg, errors.New("CALLBACK_SECRET is required when PRINCIPAL_URL is set")
	}
	var err error
	if cfg.submitTimeout, err = durationEnv("SUBMIT_TIMEOUT", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.leaseTimeout, err = durationEnv("LEASE_TIMEOUT", 2*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.recoveryInterval, err = durationEnv("RECOVERY_INTERVAL", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.leaseTimeout <= cfg.submitTimeout {
		return cfg, fmt.Errorf("LEASE_TIMEOUT (%s) must be greater than SUBMIT_TIMEOUT (%s)", cfg.leaseTimeout, cfg.submitTimeout)
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s: %q", key, v)
	}
	return d, nil
}
