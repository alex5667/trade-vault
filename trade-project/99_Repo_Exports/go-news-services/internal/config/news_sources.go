package config

import (
	"encoding/json"
	"os"
)

type ProviderFlags struct {
	Cryptopanic bool
	FMP         bool
	NewsAPI     bool
	RSS         bool
}

type NewsSourcesConfig struct {
	Providers []string          `json:"providers"`
	Raw       map[string]interface{} `json:"-"`
	Flags     ProviderFlags     `json:"-"`
	CryptoPanic CryptoPanicConfig `json:"cryptopanic"`
	FMP        FMPConfig          `json:"fmp"`
	NewsAPI    NewsAPIConfig      `json:"newsapi"`
	RSS        RSSConfig          `json:"rss"`
}

type CryptoPanicConfig struct {
	Enabled    bool     `json:"enabled"`
	Currencies []string `json:"currencies"`
	Filter     string   `json:"filter"`  // important|rising|hot|bullish|bearish|saved|lol
	Kind       string   `json:"kind"`    // news|media
	Region     string   `json:"region"`  // en, etc (в CryptoPanic реально параметр regions=en,es ...)
}

type FMPConfig struct {
	Enabled  bool     `json:"enabled"`
	Tickers  []string `json:"tickers"`
	Economic struct {
		Countries   []string `json:"countries"`   // US, EU ...
		Importance  []string `json:"importance"`  // High, Medium ...
	} `json:"economic"`
}

type NewsAPIConfig struct {
	Enabled  bool   `json:"enabled"`
	Q        string `json:"q"`
	Language string `json:"language"`
}

type RSSConfig struct {
	Enabled bool     `json:"enabled"`
	URLs    []string `json:"urls"`
}

func DefaultRSSURLs() []string {
	// Из news_sources_tables.xlsx у вас есть такие RSS/feeds (6 шт.)
	return []string{
		"https://www.ecb.europa.eu/rss/press.html",
		"https://cointelegraph.com/rss",
		"https://decrypt.co/feed",
		"https://news.bitcoin.com/feed/",
		"https://www.coindesk.com/arc/outboundfeeds/rss/",
		"https://www.newsbtc.com/feed/",
	}
}

func LoadNewsSourcesFromEnv() NewsSourcesConfig {
	rawJSON := os.Getenv("NEWS_SOURCES_JSON")
	var cfg NewsSourcesConfig
	raw := make(map[string]interface{})
	if rawJSON != "" {
		_ = json.Unmarshal([]byte(rawJSON), &cfg)
		_ = json.Unmarshal([]byte(rawJSON), &raw)
	}
	// Дефолт: rss включён с базовым набором
	if len(cfg.Providers) == 0 {
		cfg.Providers = []string{"rss"}
	}
	if !cfg.RSS.Enabled && contains(cfg.Providers, "rss") {
		cfg.RSS.Enabled = true
	}
	if cfg.RSS.Enabled && len(cfg.RSS.URLs) == 0 {
		cfg.RSS.URLs = DefaultRSSURLs()
	}
	// Авто-включение API провайдеров только при наличии ключей
	haveCP := os.Getenv("CRYPTOPANIC_AUTH_TOKEN") != ""
	haveFMP := os.Getenv("FMP_API_KEY") != ""
	haveNewsAPI := os.Getenv("NEWSAPI_KEY") != ""

	cfg.Flags = ProviderFlags{
		Cryptopanic: contains(cfg.Providers, "cryptopanic") && (cfg.CryptoPanic.Enabled || haveCP),
		FMP:         contains(cfg.Providers, "fmp") && (cfg.FMP.Enabled || haveFMP),
		NewsAPI:     contains(cfg.Providers, "newsapi") && (cfg.NewsAPI.Enabled || haveNewsAPI),
		RSS:         contains(cfg.Providers, "rss") && cfg.RSS.Enabled,
	}
	cfg.Raw = raw
	return cfg
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}