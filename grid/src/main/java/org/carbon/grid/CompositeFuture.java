package org.carbon.grid;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class CompositeFuture implements Future<Void> {

    private final Collection<Future<Void>> futures;

    CompositeFuture(Collection<Future<Void>> futures) {
        this.futures = futures;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futures.stream().map(f -> f.cancel(mayInterruptIfRunning)).reduce(true, (a,b) -> a && b);
    }

    @Override
    public boolean isCancelled() {
        return futures.stream().map(Future::isCancelled).reduce(true, (a, b) -> a && b);
    }

    @Override
    public boolean isDone() {
        return futures.stream().map(Future::isDone).reduce(true, (a, b) -> a && b);
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        for (Future<Void> f : futures) {
            f.get();
        }
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long millisToWait = MILLISECONDS.convert(timeout, unit);
        for (Future<Void> f : futures) {
            long before = System.currentTimeMillis();
            f.get(millisToWait, TimeUnit.MILLISECONDS);
            millisToWait = millisToWait - (System.currentTimeMillis() - before);
            if (millisToWait <= 0) throw new TimeoutException();
        }
        return null;
    }
}
