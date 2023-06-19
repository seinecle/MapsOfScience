package net.clementlevallois.functions.mapsofscience.similarity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class GroupedSimilarityComputer implements SimilarityComputer {
	private final GroupedInput input;
	private final ResultWriter writer;

	private final AtomicInteger next = new AtomicInteger();
	private final List<Thread> threads;

	public GroupedSimilarityComputer(GroupedInput input, ResultWriter writer, Integer jobs) {
		this.input = input;
		this.writer = writer;

		threads = new ArrayList<>();
		
		if (jobs == null) {
			jobs = Runtime.getRuntime().availableProcessors();
		}

		for (int i = 0; i < jobs; i++) {
			Thread thread = new Thread(() -> process());
			thread.start();
			threads.add(thread);
		}
	}

	@Override
	public void join() throws InterruptedException {
		for (Thread thread : threads) {
			thread.join();
		}
	}

	private void process() {
		int n;
		while ((n = next.getAndIncrement()) < input.authors.size()) {
			process(n);
		}
	}

	private void process(int n) {
		IntArrayList journals = input.authorJournals.get(n);
		int js = journals.size();
		for (int j = 0; j < js; j++) {
			int journalId = journals.getInt(j);
			IntArrayList authors = input.journalAuthors.get(journalId);
			for (int k = j + 1; k < js; k++) {
				int kournalId = journals.getInt(k);
				IntArrayList authors2 = input.journalAuthors.get(kournalId);
				computeSimilarities(n, journalId, authors, kournalId, authors2);
			}
		}
	}

	private void computeSimilarities(int n, int journalId, IntArrayList authors, int kournalId, IntArrayList authors2) {
		int jsize = authors.size();
		int ksize = authors2.size();

		int j = 0;
		int k = 0;

		int matches = 0;

		while (j < jsize && k < ksize) {
			int ja = authors.getInt(j);
			int ka = authors2.getInt(k);

			if (ja < ka) {
				j++;
			} else if (ja > ka) {
				k++;
			} else {
				if (matches == 0 && ja != n) {
					return;
				}
				matches++;
				j++;
				k++;
			}
		}
		
		writer.add(input.journals.get(journalId), input.journals.get(kournalId), matches);
	}
}
