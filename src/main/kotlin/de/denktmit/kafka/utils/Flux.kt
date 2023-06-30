package de.denktmit.kafka.utils

import reactor.core.publisher.Flux
import reactor.util.function.Tuple2
import java.util.concurrent.atomic.AtomicLong


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