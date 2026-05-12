package api

import (
	"github.com/gin-gonic/gin"
	"github.com/kireetivar/async-job-queue/store"
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

	v1 := r.engine.Group("/api/v1")
	{
		v1.POST("/jobs", r.createJob)
		v1.GET("/jobs/:id", r.getJob)
	}

	return r
}

func (r *Router) Run(addr string) error {
	return r.engine.Run(addr)
}

