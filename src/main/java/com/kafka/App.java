package com.kafka;

import com.kafka.service.FilterService;
import com.kafka.service.ReaderService;
import com.kafka.service.StatService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> {
           ReaderService readerService = new ReaderService();
           readerService.start();
        });
        executorService.submit(() -> {
//             Potential pass these values through the command line
           FilterService filterService = new FilterService(52.0d, 3.8d, 51.7d, 4.75d);
           filterService.start();
        });

        StatService statService = new StatService();
        statService.start();
    }
}
