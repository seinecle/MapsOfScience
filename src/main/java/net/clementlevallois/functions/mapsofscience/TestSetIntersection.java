/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import static net.clementlevallois.functions.mapsofscience.JournalSimilaritiesComputer.journal2AuthorsMap;

/**
 *
 * @author LEVALLOIS
 */
public class TestSetIntersection {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
         Long2ObjectOpenHashMap<ReferenceOpenHashSet<Long>> journal2AuthorsMap = new Long2ObjectOpenHashMap<>();

        
        ReferenceOpenHashSet<Long> authorsOfJournalA = new ReferenceOpenHashSet();
        long testA = 1L;
        long testB = 1L;
        authorsOfJournalA.add(testA);
        ReferenceOpenHashSet<Long> authorsOfJournalB = new ReferenceOpenHashSet();
        authorsOfJournalB.add(testB);
        
        journal2AuthorsMap.put(1, authorsOfJournalA);
        journal2AuthorsMap.put(2, authorsOfJournalB);
        
        authorsOfJournalA = journal2AuthorsMap.get(1);
        authorsOfJournalB = journal2AuthorsMap.get(2);
        
        for (Long entry: authorsOfJournalA){
            if (authorsOfJournalB.contains(entry)){
                System.out.println("stop");
            }
        }
        Set<Long> commonElements = authorsOfJournalA.parallelStream()
                .filter(authorsOfJournalB::contains)
                .collect(Collectors.toSet());
        int size = commonElements.size();
        System.out.println("size: "+ size);
    }
    
}
