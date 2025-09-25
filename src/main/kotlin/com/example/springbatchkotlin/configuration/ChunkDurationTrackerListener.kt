package com.example.springbatchkotlin.configuration

import org.springframework.batch.core.ChunkListener
import org.springframework.batch.core.scope.context.ChunkContext


class ChunkDurationTrackerListener : ChunkListener {

    override fun beforeChunk(context: ChunkContext) {
        // StepExecution에서 현재까지 커밋된 청크 수를 가져와 현재 청크 번호를 계산합니다. (0부터 시작하므로 +1)
        context.setAttribute("startTime", System.currentTimeMillis())
    }

    override fun afterChunk(context: ChunkContext) {
        val startTime = context.getAttribute("startTime") as Long
        val endTime = System.currentTimeMillis()
        val durationMillis = endTime - startTime

        // 처리가 완료된 청크 번호를 가져옵니다.
        val chunkNumber = context.stepContext.stepExecution.commitCount.toInt()

        // 밀리초 단위의 소요 시간을 시, 분, 초로 변환합니다.
        val hours = durationMillis / (1000 * 60 * 60)
        val minutes = (durationMillis % (1000 * 60 * 60)) / (1000 * 60)
        val seconds = (durationMillis % (1000 * 60)) / 1000
        val millis = durationMillis % 1000

        // 사람이 읽기 좋은 형태의 문자열로 포맷팅합니다.
        // 청크 처리 시간은 보통 짧으므로, 분 단위 이하에서는 초와 밀리초까지 보여주는 것이 유용합니다.
        val duration = when {
            hours > 0 -> {
                String.format("%d시간 %d분 %d초", hours, minutes, seconds)
            }
            minutes > 0 -> {
                String.format("%d분 %d초", minutes, seconds)
            }
            seconds > 0 -> {
                String.format("%d.%03d초", seconds, millis)
            }
            else -> {
                String.format("%dms", millis)
            }
        }

        println("Chunk #$chunkNumber duration: $duration")
    }
}