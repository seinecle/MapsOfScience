package net.clementlevallois.functions.mapsofscience.similarity;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public class OriginalSimilarityComputer implements SimilarityComputer {
	private final OriginalInput input;
	private final Long2ObjectOpenHashMap<LongOpenHashSet> journal2AuthorsMap;
	private final ResultWriter writer;

	private final Thread thread;;

	public OriginalSimilarityComputer(OriginalInput input, ResultWriter writer, Integer jobs) {
		this.input = input;
		this.journal2AuthorsMap = input.journal2AuthorsMap;
		this.writer = writer;

		thread = new Thread(() -> process());
		thread.start();
	}

	@Override
	public void join() throws InterruptedException {
		thread.join();
	}

	private void process() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		

		LongSet setOfJournalIds = journal2AuthorsMap.keySet();
		long[] arrayOfJournalIds = setOfJournalIds.toLongArray();

		IntStream.range(0, arrayOfJournalIds.length).parallel().forEach(indexJournalA -> {
			long journalIdA = arrayOfJournalIds[indexJournalA];
			LongOpenHashSet authorsA = journal2AuthorsMap.get(arrayOfJournalIds[indexJournalA]);
			IntStream.range(indexJournalA + 1, arrayOfJournalIds.length).parallel().forEach(indexJournalB -> {
				long journalIdB = arrayOfJournalIds[indexJournalB];
				executor.execute(() -> {
					int similarity = computeSimilarities(authorsA, journal2AuthorsMap.get(journalIdB));
					if (similarity > 0) {
						writer.add(String.valueOf(journalIdA), String.valueOf(journalIdB), similarity);
					}
				});

			});
		});
	}

	private int computeSimilarities(LongOpenHashSet authorsOfJournalA, LongOpenHashSet authorsOfJournalB) {
		int counterSimilarity = 0;
		if (authorsOfJournalA.size() < authorsOfJournalB.size()) {
			LongIterator iteratorA = authorsOfJournalA.iterator();
			while (iteratorA.hasNext()) {
				if (authorsOfJournalB.contains(iteratorA.nextLong())) {
					counterSimilarity++;
				}
			}
		} else {
			LongIterator iteratorB = authorsOfJournalB.iterator();
			while (iteratorB.hasNext()) {
				if (authorsOfJournalA.contains(iteratorB.nextLong())) {
					counterSimilarity++;
				}
			}
		}
		return counterSimilarity;
	}
}
