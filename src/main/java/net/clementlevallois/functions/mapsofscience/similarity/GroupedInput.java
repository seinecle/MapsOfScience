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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class GroupedInput implements Input {
	List<String> journals = new ArrayList<>();
	Object2IntOpenHashMap<String> journalIds = new Object2IntOpenHashMap<>();

	List<String> authors = new ArrayList<>();
	Object2IntOpenHashMap<String> authorIds = new Object2IntOpenHashMap<>();

	ObjectArrayList<IntArrayList> journalAuthors = new ObjectArrayList<>();
	ObjectArrayList<IntArrayList> authorJournals = new ObjectArrayList<>();

	public static GroupedInput fromFile(Path inputFilePath, Integer maxSize) throws IOException {
		GroupedInput input = new GroupedInput();
		input.parseFromFile(inputFilePath, maxSize);
		return input;
	}

	private void parseFromFile(Path inputFilePath, Integer maxSize) throws IOException {

		CharBuffer cb = CharBuffer.allocate(1 << 16);
		try (BufferedReader r = Files.newBufferedReader(inputFilePath)) {
			IntArrayList journalAuthors = new IntArrayList();
			StringBuilder sb = new StringBuilder();
			int journalId = -1;

			int read;
			while ((read = r.read(cb)) > 0) {
				cb.position(0);
				cb.limit(read);
				while (cb.hasRemaining()) {
					char c = cb.get();
					if (c == '|') {
						journalId = onJournalEnd(sb);
					} else if (c == ',' || c == '\n') {
						onAuthorEnd(sb, journalAuthors);
						if (c == '\n') {
							onLineEnd(journalId, journalAuthors);
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
				onLineEnd(journalId, journalAuthors);
			}
		}
	}

	private void onLineEnd(int journalId, IntArrayList journalAuthors) {
		journalAuthors.unstableSort(null);
		
		IntArrayList authors = new IntArrayList();
		int prev = -1;
		for (int a : journalAuthors) {
			if (a > prev) {
				prev = a;
				authors.add(a);
				authorJournals.get(a).add(journalId);
			}
		}
		
		this.journalAuthors.add(authors);
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
			authorJournals.add(new IntArrayList());
		} else {
			authorId = authorIds.getInt(author);
		}
		journalAuthors.add(authorId);
	}

	private int onJournalEnd(StringBuilder sb) {
		String journal = sb.toString();
		sb.setLength(0);

		if (!journalIds.containsKey(journal)) {
			int journalId = journals.size();
			journalIds.put(journal, journalId);
			journals.add(journal);
			return journalId;
		} else {
			return journalIds.getInt(journal);
		}
	}

	@Override
	public int size() {
		return journals.size();
	}
}
