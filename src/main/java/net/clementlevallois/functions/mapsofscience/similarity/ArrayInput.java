package net.clementlevallois.functions.mapsofscience.similarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class ArrayInput implements Input {
	List<String> journals = new ArrayList<>();
	Object2IntOpenHashMap<String> journalIds = new Object2IntOpenHashMap<>();

	List<String> authors = new ArrayList<>();
	Object2IntOpenHashMap<String> authorIds = new Object2IntOpenHashMap<>();

	IntArrayList array = new IntArrayList();
	IntArrayList starts = new IntArrayList();

	public static ArrayInput fromFile(Path inputFilePath, Integer maxSize) throws IOException {
		ArrayInput input = new ArrayInput();
		input.parseFromFile(inputFilePath, maxSize);
		return input;
	}

	private void parseFromFile(Path inputFilePath, Integer maxSize) throws IOException {

		CharBuffer cb = CharBuffer.allocate(1 << 16);
		try (BufferedReader r = Files.newBufferedReader(inputFilePath)) {
			IntArrayList journalAuthors = new IntArrayList();
			StringBuilder sb = new StringBuilder();

			int read;
			while ((read = r.read(cb)) > 0) {
				cb.position(0);
				cb.limit(read);
				while (cb.hasRemaining()) {
					char c = cb.get();
					if (c == '|') {
						onJournalEnd(sb);
					} else if (c == ',' || c == '\n') {
						onAuthorEnd(sb, journalAuthors);
						if (c == '\n') {
							onLineEnd(journalAuthors);
							if (maxSize != null && journals.size() >= maxSize) {
								return;
							}
						}
					} else if (c >= '0' && c <= '9') {
						sb.append(c);
					} else if (c == '\r') {
						// skip
					}
				}
				cb.clear();
			}

			if (!sb.isEmpty()) {
				onAuthorEnd(sb, journalAuthors);
				onLineEnd(journalAuthors);
			}
		}
	}

	private void onLineEnd(IntArrayList journalAuthors) {
		int ptr = array.size();
		starts.add(ptr);
		array.add(-1);
		journalAuthors.unstableSort(null);
		
		int prev = -1;
		int cnt = 0;
		for (int a : journalAuthors) {
			if (a > prev) {
				prev = a;
				array.add(a);
				cnt++;
			}
		}
		
		array.set(ptr, cnt);
		journalAuthors.clear();
	}

	private void onAuthorEnd(StringBuilder sb, IntArrayList journalAuthors) {
		String author = sb.toString();
		sb.setLength(0);

		int authorId;
		if (!authorIds.containsKey(author)) {
			authorId = authors.size();
			authorIds.put(author, authorId);
			authors.add(author);
		} else {
			authorId = authorIds.getInt(author);
		}
		journalAuthors.add(authorId);
	}

	private void onJournalEnd(StringBuilder sb) {
		String journal = sb.toString();
		sb.setLength(0);

		if (!journalIds.containsKey(journal)) {
			journalIds.put(journal, journals.size());
			journals.add(journal);
		}
	}

	@Override
	public int size() {
		return journals.size();
	}
}
