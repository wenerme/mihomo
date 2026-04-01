package route

import (
	"context"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func adaptiveRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getAdaptiveMetrics)
	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByNameForAdaptive)
		r.Get("/", getProxyAdaptiveMetrics)
	})
	return r
}

type AdaptiveMetrics struct {
	Latency     float64 `json:"latency"`
	FailureRate float64 `json:"failureRate"`
	Jitter      float64 `json:"jitter"`
	Samples     int     `json:"samples"`
}

func getAdaptiveMetrics(w http.ResponseWriter, r *http.Request) {
	proxies := tunnel.Proxies()
	result := make(map[string]*AdaptiveMetrics)

	for name := range proxies {
		latency, failureRate, jitter, samples := outboundgroup.GetProxyAdaptiveState(name)
		if samples > 0 {
			result[name] = &AdaptiveMetrics{
				Latency:     latency,
				FailureRate: failureRate,
				Jitter:      jitter,
				Samples:     samples,
			}
		}
	}

	render.JSON(w, r, render.M{
		"proxies": result,
	})
}

func findProxyByNameForAdaptive(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.Context().Value(CtxKeyProxyName).(string)
		proxies := tunnel.Proxies()
		_, exist := proxies[name]
		if !exist {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}

		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getProxyAdaptiveMetrics(w http.ResponseWriter, r *http.Request) {
	name := r.Context().Value(CtxKeyProxyName).(string)
	latency, failureRate, jitter, samples := outboundgroup.GetProxyAdaptiveState(name)

	render.JSON(w, r, &AdaptiveMetrics{
		Latency:     latency,
		FailureRate: failureRate,
		Jitter:      jitter,
		Samples:     samples,
	})
}
