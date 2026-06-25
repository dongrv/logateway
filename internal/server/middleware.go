package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-Id")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-Id", requestID)
		c.Next()
	}
}

func LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		reqIDVal, _ := c.Get("request_id")
		requestID, ok := reqIDVal.(string)
		if !ok || requestID == "" {
			requestID = "unknown"
		}

		traceID := c.GetHeader("X-Trace-Id")
		if traceID == "" {
			traceID = requestID
		}

		logJSON("info", "request", requestID, traceID, "",
			fmt.Sprintf("method=%s path=%s status=%d duration=%s",
				c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start)))
	}
}

func BodyCacheMiddleware(cfgMgr *config.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := cfgMgr.Get()
		maxBytes := cfg.Server.MaxBodyBytes
		if maxBytes <= 0 {
			maxBytes = 1 << 20
		}

		reader := http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes+1)
		rawBody, err := io.ReadAll(reader)
		c.Request.Body.Close()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code:    http.StatusRequestEntityTooLarge,
				Message: "request body too large",
			})
			return
		}
		if int64(len(rawBody)) > maxBytes {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code:    http.StatusRequestEntityTooLarge,
				Message: "request body too large",
			})
			return
		}

		c.Request.Body = io.NopCloser(bytes.NewReader(rawBody))
		c.Set("raw_body", rawBody)
		c.Next()
	}
}

func ProjectResolutionMiddleware(cfgMgr *config.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		var rawBody []byte
		if b, exists := c.Get("raw_body"); exists {
			var ok bool
			rawBody, ok = b.([]byte)
			if !ok {
				log.Printf("[ERROR] raw_body is not []byte, got %T", b)
				c.JSON(http.StatusInternalServerError, message.UploadResponse{
					Code:    http.StatusInternalServerError,
					Message: "internal error",
				})
				c.Abort()
				return
			}
		} else {
			var err error
			rawBody, err = c.GetRawData()
			if err != nil {
				c.JSON(http.StatusBadRequest, message.UploadResponse{
					Code:    http.StatusBadRequest,
					Message: "failed to read body",
				})
				c.Abort()
				return
			}
			c.Set("raw_body", rawBody)
		}

		var peek struct {
			Project string `json:"project"`
		}
		if err := json.Unmarshal(rawBody, &peek); err != nil || peek.Project == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, message.UploadResponse{
				Code:    http.StatusBadRequest,
				Message: "Project is required",
			})
			return
		}

		projCfg := cfgMgr.GetProject(peek.Project)
		if projCfg == nil {
			c.AbortWithStatusJSON(http.StatusNotFound, message.UploadResponse{
				Code:    http.StatusNotFound,
				Message: fmt.Sprintf("unknown project: %s", peek.Project),
			})
			return
		}

		if projCfg.AuthRequired {
			if _, exists := c.Get("app_key"); !exists {
				c.AbortWithStatusJSON(http.StatusUnauthorized, message.UploadResponse{
					Code:    http.StatusUnauthorized,
					Message: "authentication required",
				})
				return
			}
		}

		c.Set("project_name", projCfg.Name)
		c.Next()
	}
}

func logJSON(level, msg, requestID, traceID, project, errStr string) {
	entry := struct {
		Timestamp string `json:"timestamp"`
		Level     string `json:"level"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
		TraceID   string `json:"trace_id,omitempty"`
		Project   string `json:"project,omitempty"`
		Error     string `json:"error,omitempty"`
	}{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Message:   msg,
		RequestID: requestID,
		TraceID:   traceID,
		Project:   project,
		Error:     errStr,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[ERROR] log marshal failed: %v (original: %s %s)", err, level, msg)
		return
	}
	log.Println(string(data))
}
