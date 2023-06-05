/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.debug;

import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class ArrayExperiments {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Clock clock = new Clock("test");
        int arraySize = Integer.MAX_VALUE - 10;
        System.out.println("heapMaxSize " + Runtime.getRuntime().maxMemory());
        int[] test = new int[arraySize];
        for (int i = 0; i < (arraySize); i++) {
            test[i] = ((Long) Math.round(Math.random() * 1_000_000)).intValue();
            if (i++ % 10_000_000 == 0) {
                System.out.print("count: " + i);
                System.out.print(", ");
                clock.printElapsedTime();
// Get the Java runtime
                Runtime runtime = Runtime.getRuntime();
                // Run the garbage collector
                runtime.gc();
                // Calculate the used memory
                long memory = runtime.totalMemory() - runtime.freeMemory();
                System.out.println("Used memory is megabytes: " + (double) memory / (1024 * 1024));
            }
        }
        clock.closeAndPrintClock();
        System.out.println(test.length);

    }

}
