package net.clementlevallois.functions.mapsofscience.similarity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.clementlevallois.functions.mapsofscience.Constants;

public class OriginalInput implements Input {
	Long2ObjectOpenHashMap<LongOpenHashSet> journal2AuthorsMap = new Long2ObjectOpenHashMap<>();

	public static OriginalInput fromFile(Path inputFilePath, Integer maxSize) throws IOException {
		OriginalInput input = new OriginalInput();
		input.parseFromFile(inputFilePath, maxSize);
		return input;
	}

	private void parseFromFile(Path inputFilePath, Integer maxSize) throws IOException {
		List<String> lines = Files.readAllLines(inputFilePath);

		if (maxSize == null || maxSize > lines.size()) {
			maxSize = lines.size();
		}

		for (int i = 0; i < maxSize; i++) {
			processLine(lines.get(i));
		}
	}

	private void processLine(String line) {
		String fields[] = line.split("\\" + Constants.INTER_FIELD_SEPARATOR);
		if (fields.length < 2) {
			return;
		}
		String journalId = fields[0];
		long journalIdAsLong = Long.parseLong(journalId);
		String authorIdsAsLine = fields[1];
		String authorIds[] = authorIdsAsLine.split(Constants.INTRA_FIELD_SEPARATOR);
		LongOpenHashSet setOfCurrentAuthors = new LongOpenHashSet();
		LongOpenHashSet setOfAuthorsForThisJournal = journal2AuthorsMap.getOrDefault(journalIdAsLong,
				setOfCurrentAuthors);
		for (String authorId : authorIds) {
			try {
				long authorIdLong = Long.parseLong(authorId);
				setOfAuthorsForThisJournal.add(authorIdLong);
			} catch (NumberFormatException e) {
				System.out.println(line);
				System.out.println("error with author id, not  long: " + authorId);
			}
		}
		journal2AuthorsMap.put(journalIdAsLong, setOfAuthorsForThisJournal);
	}

	@Override
	public int size() {
		return journal2AuthorsMap.size();
	}
}
