package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Proxy struct {
	monolithURL      *url.URL
	moviesURL        *url.URL
	eventsURL        *url.URL
	gradualMigration bool
	migrationPercent int
}

func NewProxy() (*Proxy, error) {
	// Читаем переменные окружения
	monolithURLStr := os.Getenv("MONOLITH_URL")
	if monolithURLStr == "" {
		monolithURLStr = "http://monolith:8080"
	}
	monolithURL, err := url.Parse(monolithURLStr)
	if err != nil {
		return nil, fmt.Errorf("invalid MONOLITH_URL: %w", err)
	}

	moviesURLStr := os.Getenv("MOVIES_SERVICE_URL")
	if moviesURLStr == "" {
		moviesURLStr = "http://movies-service:8081"
	}
	moviesURL, err := url.Parse(moviesURLStr)
	if err != nil {
		return nil, fmt.Errorf("invalid MOVIES_SERVICE_URL: %w", err)
	}

	eventsURLStr := os.Getenv("EVENTS_SERVICE_URL")
	if eventsURLStr == "" {
		eventsURLStr = "http://events-service:8082"
	}
	eventsURL, err := url.Parse(eventsURLStr)
	if err != nil {
		return nil, fmt.Errorf("invalid EVENTS_SERVICE_URL: %w", err)
	}

	gradual := os.Getenv("GRADUAL_MIGRATION") == "true"

	percent := 0
	if p := os.Getenv("MOVIES_MIGRATION_PERCENT"); p != "" {
		percent, err = strconv.Atoi(p)
		if err != nil {
			log.Printf("Warning: invalid MOVIES_MIGRATION_PERCENT, using 0")
			percent = 0
		}
		if percent < 0 || percent > 100 {
			percent = 0
		}
	}

	return &Proxy{
		monolithURL:      monolithURL,
		moviesURL:        moviesURL,
		eventsURL:        eventsURL,
		gradualMigration: gradual,
		migrationPercent: percent,
	}, nil
}

// hashIP возвращает число от 0 до 99 на основе IP-адреса
func (p *Proxy) hashIP(ip string) int {
	if ip == "" {
		ip = "unknown"
	}
	hash := md5.Sum([]byte(ip))
	hexHash := hex.EncodeToString(hash[:])
	// Берём последние два символа шестнадцатеричной строки и преобразуем в int
	val, _ := strconv.ParseInt(hexHash[len(hexHash)-2:], 16, 64)
	return int(val % 100)
}

// shouldGoToMoviesService определяет, куда направить запрос к /movies
func (p *Proxy) shouldGoToMoviesService(r *http.Request) bool {
	if !p.gradualMigration {
		return false // весь трафик идёт в монолит
	}

	// Получаем IP клиента (учитываем возможный X-Forwarded-For)
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.RemoteAddr
	}
	// Убираем порт, если есть
	if strings.Contains(ip, ":") {
		ip = strings.Split(ip, ":")[0]
	}

	hash := p.hashIP(ip)
	return hash < p.migrationPercent
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Определяем целевой сервис по пути
	if strings.HasPrefix(r.URL.Path, "/api/movies") {
		if p.shouldGoToMoviesService(r) {
			// Направляем в movies-service
			p.proxyTo(p.moviesURL, w, r)
			return
		}
		// Иначе в монолит
		p.proxyTo(p.monolithURL, w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/events") {
		p.proxyTo(p.eventsURL, w, r)
		return
	}

	// Все остальные запросы — в монолит
	p.proxyTo(p.monolithURL, w, r)
}

func (p *Proxy) proxyTo(target *url.URL, w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(target)
	// Сохраняем оригинальный путь запроса
	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Host = target.Host
	proxy.ServeHTTP(w, r)
}

func main() {
	proxy, err := NewProxy()
	if err != nil {
		log.Fatalf("Failed to initialize proxy: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	log.Printf("Starting proxy service on port %s", port)
	log.Printf("Gradual migration: %v, migration percent: %d%%", proxy.gradualMigration, proxy.migrationPercent)

	if err := http.ListenAndServe(":"+port, proxy); err != nil {
		log.Fatal(err)
	}
}
