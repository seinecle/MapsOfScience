package net.clementlevallois.functions.mapsofscience.similarity;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileResultWriter implements ResultWriter {
	private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1 << 16);
	private volatile boolean end;
	private final Thread thread;

	public FileResultWriter(Path outputFilePath) {
		try {
			Files.createDirectories(outputFilePath.getParent());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		thread = new Thread(() -> write(outputFilePath));
		thread.start();
	}

	private void write(Path outputFilePath) {
		try (BufferedWriter writer = Files.newBufferedWriter(outputFilePath, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.CREATE)) {
			while (!end) {
				String line;
				while ((line = queue.poll(1, TimeUnit.MILLISECONDS)) != null) {
					writer.write(line);
				}
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void add(String string) {
		try {
			queue.put(string);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void join() throws InterruptedException {
		end = true;
		thread.join();
	}
}
