package no.ssb.vtl.tools.sandbox.connector.spring;

import no.ssb.vtl.model.DataPoint;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Created by hadrien on 21/06/2017.
 */
class BlockingQueueSpliterator extends Spliterators.AbstractSpliterator<DataPoint> {

    // End of stream marker.
    public static final DataPoint EOS = DataPoint.create(0);

    private final BlockingQueue<DataPoint> queue;
    private final Future<?> future;

    public BlockingQueueSpliterator(BlockingQueue<DataPoint> queue, Future<?> future) {
        super(Long.MAX_VALUE, Spliterator.IMMUTABLE);
        this.queue = queue;
        this.future = future;
    }

    @Override
    public boolean tryAdvance(Consumer<? super DataPoint> action) {
        try {

            DataPoint p = queue.take();
            if (p == EOS)
                return false;

            action.accept(p);

        } catch (InterruptedException ie) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("stream interrupted");
        }
        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super DataPoint> action) {
        try {

            DataPoint p;
            while ((p = queue.take()) != EOS)
                action.accept(p);

        } catch (InterruptedException ie) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("stream interrupted");
        }
    }
}
