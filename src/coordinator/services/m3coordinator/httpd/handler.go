package httpd

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"go.uber.org/zap"
)

// Handler represents an HTTP handler.
type Handler struct {
	Router    *mux.Router
	CLFLogger *log.Logger
	storage   storage.Storage
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(storage storage.Storage) (*Handler, error) {
	r := mux.NewRouter()
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	defer logger.Sync() // flushes buffer, if any
	h := &Handler{
		CLFLogger: log.New(os.Stderr, "[httpd] ", 0),
		Router:    r,
		storage:   storage,
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging
	h.Router.HandleFunc(handler.PromReadURL, logged(handler.NewPromReadHandler(h.storage)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(handler.PromWriteURL, logged(handler.NewPromWriteHandler(h.storage)).ServeHTTP).Methods("POST")
}

func withResponseTimeLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		// Attach a rqID with all logs so that its simple to trace the whole call stack
		rqID := uuid.NewRandom()
		rqCtx := logging.NewContext(r.Context(), zap.Stringer("rqID", rqID))
		logger := logging.WithContext(rqCtx)
		// Propagate the context with the reqId
		next.ServeHTTP(w, r.WithContext(rqCtx))
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			logger.Info("finished handling request", zap.Time("time", endTime), zap.Duration("response", d), zap.String("url", r.URL.RequestURI()))
		}
	})
}