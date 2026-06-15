package api

import (
	"github.com/gin-gonic/gin"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

type Router struct {
	store  store.Store
	engine *gin.Engine
}

func NewRouter(store store.Store) *Router {
	r := &Router{
		store:  store,
		engine: gin.Default(),
	}

	// Swagger UI served at /swagger/index.html
	r.engine.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.engine.GET("/health", r.healthCheck)
	r.engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := r.engine.Group("/api/v1")
	{
		v1.POST("/jobs", r.createJob)
		v1.GET("/jobs/:id", r.getJob)
		v1.DELETE("/jobs/:id", r.deleteJob)
		v1.GET("/queues", r.getQueues)
		v1.POST("/queues/:name/pause", r.pauseQueue)
		v1.POST("/queues/:name/resume", r.resumeQueue)
		v1.GET("/dashboard", r.getHealth)
		v1.POST("jobs/:id/retry", r.retryJob)
	}

	return r
}

func (r *Router) Run(addr string) error {
	return r.engine.Run(addr)
}
