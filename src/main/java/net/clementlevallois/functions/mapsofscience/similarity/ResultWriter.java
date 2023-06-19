package net.clementlevallois.functions.mapsofscience.similarity;

import java.io.IOException;
import java.nio.file.Path;

public interface ResultWriter {
	ResultWriter DUMMY = new ResultWriter() {
		@Override
		public void join() throws InterruptedException {
		}

		@Override
		public void add(String string) {
		}
		
		@Override
		public void add(String j1, String j2, int m) {
		}
	};

	void add(String string);
	
	default void add(String j1, String j2, int m) {
		if (j1.compareTo(j2) > 0) {
			add(j1 + "," + j2 + "," + m + "\n");
		} else {
			add(j2 + "," + j1 + "," + m + "\n");
		}
	}

	void join() throws InterruptedException;

	static ResultWriter forOutput(String path, String algorithm, Integer limit, Integer jobs) throws IOException {
		if (path == null) {
			return DUMMY;
		}

		Path outputFilePath = Path.of(path.replace("$$", "$").replace("$alg", algorithm)
				.replace("$limit", limit == null ? "unlimited" : String.valueOf(limit))
				.replace("$jobs", jobs == null ? "unspecified" : String.valueOf(jobs)));

		return new FileResultWriter(outputFilePath);
	}
}
