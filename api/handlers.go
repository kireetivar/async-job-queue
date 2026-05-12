package api

import (
	"net/http"
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/kireetivar/async-job-queue/models"
)

type CreateJob struct {
	Queue      string          `json:"queue" binding:"required"`
	Type       string          `json:"type" binding:"required"`
	Payload    json.RawMessage `json:"payload"`
	Priority   int             `json:"priority"`
	MaxRetries int             `json:"max_retries"`
}


func (r *Router) createJob(c *gin.Context) {
	var req CreateJob
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{ "error": err.Error()})
		return
	} 

	jobId := uuid.New().String()

	job := &models.Job{
		ID: jobId,
		Queue: req.Queue,
		Type: req.Type,
		Payload: req.Payload,
		Priority: req.Priority,
		MaxRetries: req.MaxRetries,
	}

	if err := r.store.Enqueue(c, job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, job)

}

func (r *Router) getJob(c *gin.Context) {
	jobId := c.Param("id")
	job, err := r.store.GetJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	c.JSON(http.StatusOK, job)
}