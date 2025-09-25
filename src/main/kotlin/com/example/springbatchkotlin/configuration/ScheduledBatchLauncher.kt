package com.example.springbatchkotlin.configuration

import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class ScheduledBatchLauncher(
    private val jobLauncher: JobLauncher,
    private val accountJob: Job
) {

    @Scheduled(cron = "*/10 * * * * ?")  //
    fun launchJob() {
        val params = JobParametersBuilder()
            .addLong("run.id", System.currentTimeMillis())
            .toJobParameters()
        jobLauncher.run(accountJob, params)
    }
}