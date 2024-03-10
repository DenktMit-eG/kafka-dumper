package de.denktmit.kafka.utils

import reactor.core.publisher.Flux
import reactor.util.function.Tuple2
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.roundToLong


fun <T> Flux<T>.logEveryNthObservable(
    logNthFunction: (rowNum: Long, observable: T) -> Unit,
    logAfterTerminateFunction: (rowNum: Long) -> Unit,
    logEveryNth: Int = 10
): Flux<T> {
    val latestRowNum = AtomicLong(0)
    return this.index()
        .doOnNext { indexAndObservable: Tuple2<Long, T> ->
            val currentRow = indexAndObservable.t1 + 1
            val observable = indexAndObservable.t2
            latestRowNum.set(currentRow)
            if (currentRow > 0 && currentRow % logEveryNth == 0L) {
                logNthFunction(currentRow, observable)
            }
        }.doOnTerminate {
            logAfterTerminateFunction(latestRowNum.get())
        }
        .map { obj: Tuple2<Long?, T> -> obj.t2 }
}

fun <T> Flux<T>.logThroughputEveryDuration(
    duration: Duration = Duration.ofSeconds(10),
    logThroughputFunction: (throughput: Long, totalMessages: Long) -> Unit,
): Flux<T> {
    val messageCount = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    return this.doOnNext { _ ->
        messageCount.incrementAndGet()
    }
        .sample(duration)
        .doOnNext {
            val currentTime = System.currentTimeMillis()
            val elapsedTimeInSeconds = (currentTime - startTime) / 1000.0
            val totalMessages = messageCount.get()
            val throughput = (totalMessages / elapsedTimeInSeconds).roundToLong()

            logThroughputFunction(throughput, totalMessages)
        }
        .doOnTerminate {
            val totalTimeInSeconds = (System.currentTimeMillis() - startTime) / 1000.0
            val totalMessages = messageCount.get()
            val finalThroughput = (totalMessages / totalTimeInSeconds).roundToLong()

            logThroughputFunction(finalThroughput, totalMessages)
        }
}
