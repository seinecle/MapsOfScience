/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.similarity;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import net.clementlevallois.utils.Clock;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class JournalSimilaritiesComputer {
	interface InputFactory<I extends Input> {
		I create(Path inputFilePath, Integer limit) throws IOException;
	}

	interface ComputerFactory<I extends Input, C extends SimilarityComputer> {
		C create(I input, ResultWriter writer, Integer jobs);
	}

	record Algorithm<I extends Input, C extends SimilarityComputer>(InputFactory<I> parser,
			ComputerFactory<I, C> computerFactory) {
	}

	static final Map<String, Algorithm<?, ?>> algorithms = Map.of("original",
			new Algorithm<>(OriginalInput::fromFile, OriginalSimilarityComputer::new), "array",
			new Algorithm<>(ArrayInput::fromFile, ArraySimilarityComputer::new), "another-array",
			new Algorithm<>(AnotherArrayInput::fromFile, AnotherArraySimilarityComputer::new), "grouped",
			new Algorithm<>(GroupedInput::fromFile, GroupedSimilarityComputer::new));

	public static void main(String[] args) throws IOException, InterruptedException {
		ArgumentParser parser = ArgumentParsers.newFor("JournalSimilaritiesComputer").build().defaultHelp(true)
				.description("Calculate similarities between journals");

		parser.addArgument("-o", "--output").metavar("FILE").type(String.class).help(
				"Output file path ($$ denotes a single $, $alg denotes used algorithm, $limit denotes the limit, $jobs denotes the jobs)");

		parser.addArgument("-l", "--limit").metavar("NUM_JOURNALS").type(Integer.class).setDefault(0)
				.help("Limit to processsing only the first NUM_JOURNALS; zero for unlimited number");

		parser.addArgument("-a", "--algorithms").metavar("ALG").type(String.class).required(true).nargs("+")
				.choices("original", "array", "another-array", "grouped").help("Algorithm(s) to use");

		parser.addArgument("-j", "--jobs", "--parallelism").metavar("JOBS").type(Integer.class).setDefault(0)
				.help("Number of threads (jobs) to use; zero for unspecified number");

		parser.addArgument("input").metavar("FILE").type(String.class).required(true).help("Input file path");

		Namespace ns = parser.parseArgsOrFail(args);

		String outputFilePath = ns.get("output");
		Integer limit = ns.get("limit");
		if (limit <= 0) {
			limit = null;
		}
		List<String> algorithms = ns.get("algorithms");
		Integer jobs = ns.get("jobs");
		if (jobs <= 0) {
			jobs = null;
		}
		Path inputFilePath = Paths.get(ns.<String>get("input"));

		for (String alg : algorithms) {
			Algorithm<?, ?> algorithm = JournalSimilaritiesComputer.algorithms.get(alg);
			
			System.out.println("running algorithm: " + alg);

			Clock parsing = new Clock("parsing input");
			Input input = algorithm.parser().create(inputFilePath, limit);
			parsing.closeAndPrintClock();

			System.out.println("number of entries in the map: " + input.size());

			Clock processing = new Clock("computing similarities");
			ResultWriter writer = ResultWriter.forOutput(outputFilePath, alg, limit, jobs);
			SimilarityComputer computer = ((ComputerFactory<Input, SimilarityComputer>)algorithm.computerFactory()).create(input, writer, jobs);

			computer.join();
			writer.join();
			processing.closeAndPrintClock();
		}
	}
}
