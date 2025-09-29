package com.example.springbatchkotlin.job

import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import java.time.Duration
import java.time.LocalDateTime


class StepDurationTrackerListener : StepExecutionListener {

    override fun beforeStep(stepExecution: StepExecution) {
        println(">>> Step 시작: ${stepExecution.stepName} (Job=${stepExecution.jobExecution.jobInstance.jobName}, 시작 시각: ${stepExecution.startTime})")
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus {
        val startTime: LocalDateTime = stepExecution.startTime!!
        val endTime: LocalDateTime = stepExecution.endTime!!
        val durationMillis = Duration.between(startTime, endTime).toMillis()

        val hours = durationMillis / (1000 * 60 * 60)
        val minutes = (durationMillis % (1000 * 60 * 60)) / (1000 * 60)
        val seconds = (durationMillis % (1000 * 60)) / 1000

        val duration = if (hours > 0) {
            String.format("%d시간 %d분", hours, minutes)
        } else if (minutes > 0) {
            String.format("%d분", minutes)
        } else {
            String.format("%d초", seconds)
        }

        println(">>> Step 종료: ${stepExecution.stepName}, 상태=${stepExecution.status}, 읽음=${stepExecution.readCount}건, 처리=${stepExecution.processSkipCount + stepExecution.writeCount}건, 기록=${stepExecution.writeCount}건, 스킵=${stepExecution.skipCount}건, 소요시간=${duration}")

        // 스킵 발생 시 커스텀 ExitStatus 설정
        if (stepExecution.skipCount > 0) {
            println(">>> Step 내 일부 아이템 처리 누락(스킵) 발생")
            return ExitStatus("COMPLETED WITH SKIPS")
        }
        return stepExecution.exitStatus
    }
}