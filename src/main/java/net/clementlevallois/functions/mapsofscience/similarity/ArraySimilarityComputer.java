package net.clementlevallois.functions.mapsofscience.similarity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class ArraySimilarityComputer implements SimilarityComputer {
	private final ArrayInput input;
	private final IntArrayList array;
	private final ResultWriter writer;

	private final AtomicInteger next = new AtomicInteger();
	private final List<Thread> threads;

	public ArraySimilarityComputer(ArrayInput input, ResultWriter writer, Integer jobs) {
		this.input = input;
		this.array = input.array;
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

	public void join() throws InterruptedException {
		for (Thread thread : threads) {
			thread.join();
		}
	}

	private void process() {
		int n;
		while ((n = next.getAndIncrement()) < input.starts.size()) {
			int first = input.starts.getInt(n);
			process(n, first);
		}
	}

	private void process(int n, int first) {
		int second = first + array.getInt(first) + 1;
		int m = n + 1;
		while (second < array.size()) {
			computeSimilarities(n, first, m, second);
			second += array.getInt(second) + 1;
			m++;
		}
	}

	private void computeSimilarities(int n, int first, int m, int second) {
		int flast = first + array.getInt(first);
		int slast = second + array.getInt(second);

		int f = first + 1;
		int s = second + 1;

		int matches = 0;

		while (f <= flast && s <= slast) {
			int fa = array.getInt(f);
			int sa = array.getInt(s);

			if (fa < sa) {
				f++;
			} else if (fa > sa) {
				s++;
			} else {
				matches++;
				f++;
				s++;
			}
		}

		if (matches > 0) {
			writer.add(input.journals.get(n), input.journals.get(m), matches);
		}
	}
}
