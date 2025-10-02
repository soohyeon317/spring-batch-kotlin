package com.example.springbatchkotlin.job

import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import java.time.Duration


class JobDurationTrackerListener : JobExecutionListener {
    override fun beforeJob(jobExecution: JobExecution) {
        println(">>> Job 시작: ${jobExecution.getJobInstance().getJobName()} (시작 시각: ${jobExecution.getStartTime()})",)
    }

    override fun afterJob(jobExecution: JobExecution) {
        val startTime = jobExecution.startTime!!
        val endTime = jobExecution.endTime!!
        val durationMillis = Duration.between(startTime, endTime).toMillis()
        val hours = durationMillis / (1000 * 60 * 60)
        val minutes = (durationMillis % (1000 * 60 * 60)) / (1000 * 60)
        val seconds = (durationMillis % (1000 * 60)) / 1000

        val duration: String?
        if (hours > 0) {
            duration = String.format("%d시간 %d분", hours, minutes)
        } else if (minutes > 0) {
            duration = String.format("%d분", minutes)
        } else {
            duration = String.format("%d초", seconds)
        }

        println(">>> Job 종료: 상태=${jobExecution.status}, 총 소요시간=${duration}, 종료 시각=${jobExecution.endTime}")

        if (jobExecution.getStatus().isUnsuccessful) {
            println(">>> Job 실패 원인: ${jobExecution.allFailureExceptions}")
        }
    }
}