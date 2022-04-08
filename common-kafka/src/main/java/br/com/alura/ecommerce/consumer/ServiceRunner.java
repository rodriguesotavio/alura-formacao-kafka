package br.com.alura.ecommerce.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceRunner<T> {

    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<T>(factory);
    }

    public void start(int threadCount) {
        ExecutorService poll = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i <= threadCount; i++) {
            poll.submit(provider);
        }
    }
}