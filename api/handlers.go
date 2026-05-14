package api

import (
	"encoding/json"
	"net/http"
	"time"

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
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jobId := uuid.New().String()

	job := &models.Job{
		ID:         jobId,
		Queue:      req.Queue,
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		Status:     models.StatusEnqueued,
		CreatedAt:  time.Now(),
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

func (r *Router) deleteJob(c *gin.Context) {
	jobId := c.Param("id")
	err := r.store.CancelJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func (r *Router) getQueues(c *gin.Context) {
	queues, err := r.store.ListQueues(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, queues)
}

func (r *Router) resumeQueue(c *gin.Context) {
	name := c.Param("name")
	err := r.store.ResumeQueue(c, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func (r *Router) pauseQueue(c *gin.Context) {
	name := c.Param("name")
	err := r.store.PauseQueue(c, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func (r *Router) getHealth(c *gin.Context) {
	queueStatus, err := r.store.GetQueueStatus(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, queueStatus)
}

func (r *Router) retryJob(c *gin.Context) {
	jobId := c.Param("id")
	job, err := r.store.GetJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	status := job.Status
	if status != models.StatusDead && status != models.StatusFailed {
		c.JSON(http.StatusNotAcceptable, gin.H{"error": "can only retry dead or failed jobs"})
		return
	}
	job.RetryCount = 0
	job.Status = models.StatusEnqueued
	err = r.store.Enqueue(c, job)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "retry successful"})
}
